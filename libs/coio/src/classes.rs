//! Buffer size classes and the packed-bid layout shared by the provided and
//! fixed buffer pools.
//!
//! Each direction's pool memory is split into a user-defined set of size
//! classes; a class is a run of equal-sized slots. A
//! [`crate::pbuf::ProvidedBuffer`]/[`crate::buf::BufferBytes`] smart
//! pointer identifies its slot with a packed `bid`: the top byte is the class
//! index and the low 24 bits are the slot's id within that class.

/// A run of equal-sized buffer slots.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SizeClass {
    /// Slot size in bytes.
    pub size: u32,
    /// Number of slots in the class.
    pub count: u32,
}

/// The default class table, used by [`crate::runtime::RuntimeParams::default`].
pub const DEFAULT_SIZE_CLASSES: [SizeClass; 6] = [
    SizeClass {
        size: 64,
        count: 2048,
    },
    SizeClass {
        size: 256,
        count: 2048,
    },
    SizeClass {
        size: 1024,
        count: 512,
    },
    SizeClass {
        size: 4096,
        count: 128,
    },
    SizeClass {
        size: 16384,
        count: 16,
    },
    SizeClass {
        size: 65536,
        count: 2,
    },
];

pub(crate) const BID_CLASS_SHIFT: u32 = 24;
pub(crate) const BID_LOCAL_MASK: u32 = 0x00FF_FFFF;

/// The maximum alignment any buffer can have, in bytes: one page. A buffer's
/// alignment is `min(size, BUFFER_MAX_ALIGN)`, so no allocation ever asks the
/// allocator for — or hands out — an alignment beyond a page.
pub(crate) const BUFFER_MAX_ALIGN: u32 = 4096;

/// Packs a class index and a local slot id into the global `bid` carried by a
/// buffer smart pointer.
pub(crate) fn pack_bid(class: u32, local: u32) -> u32 {
    debug_assert!(class < 256);
    debug_assert!(local <= BID_LOCAL_MASK);
    (class << BID_CLASS_SHIFT) | local
}

/// The class index stored in the top byte of a packed `bid`.
pub(crate) fn bid_class(bid: u32) -> u32 {
    bid >> BID_CLASS_SHIFT
}

/// The slot's id within its class, stored in the low 24 bits of a packed `bid`.
pub(crate) fn bid_local(bid: u32) -> u32 {
    bid & BID_LOCAL_MASK
}

/// Returns the index of the smallest class whose slot size is at least `size`.
///
/// `classes` must be sorted by ascending size.
pub(crate) fn class_for(classes: &[SizeClass], size: usize) -> Option<usize> {
    classes.iter().position(|class| class.size as usize >= size)
}

/// The validated layout of one class inside a direction's shared slab.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SizeClassLayout {
    pub size: u32,
    pub count: u32,
    /// Byte offset of the class's first slot within the shared slab. A
    /// multiple of `min(size, BUFFER_MAX_ALIGN)`, so — given a slab base that
    /// is itself aligned to every class's alignment — every slot in the class
    /// is aligned to the buffer alignment it hands out.
    pub base_offset: u32,
    /// Cumulative slab bytes through the end of this class (including the
    /// alignment padding before the class).
    pub total: u64,
}

/// Sorts `classes` by ascending size, validates the layout, and returns the
/// per-class base offsets and cumulative totals. The returned order is the
/// class-index order used by the packed `bid` and by `buf_group`.
///
/// Each class starts on an offset aligned to its slot size capped at one page,
/// so slots of size `S` land on `min(S, BUFFER_MAX_ALIGN)`-aligned addresses
/// once the slab base is aligned to the largest class's alignment. The gap
/// between consecutive classes is left unused.
///
/// With `power_of_two_counts`, every `count` must be a power of two no larger
/// than `32768`, matching the kernel's provided-buffer-ring limit.
pub(crate) fn layout_classes(
    classes: &[SizeClass],
    power_of_two_counts: bool,
) -> Vec<SizeClassLayout> {
    assert!(
        !classes.is_empty(),
        "at least one buffer size class is required"
    );
    assert!(
        classes.len() <= 256,
        "no more than 256 buffer size classes are supported (the packed bid has one class byte)"
    );

    let mut sorted: Vec<SizeClass> = classes.to_vec();
    sorted.sort_by_key(|class| class.size);

    for pair in sorted.windows(2) {
        assert!(
            pair[0].size < pair[1].size,
            "buffer size class sizes must be strictly increasing"
        );
    }

    let mut layouts = Vec::with_capacity(sorted.len());
    let mut base: u64 = 0;
    for class in &sorted {
        assert!(class.size > 0, "buffer size class size must be nonzero");
        assert!(class.count > 0, "buffer size class count must be nonzero");
        assert!(
            u64::from(class.count) <= u64::from(BID_LOCAL_MASK),
            "buffer size class count exceeds the packed-bid local range"
        );
        if power_of_two_counts {
            assert!(
                class.count.is_power_of_two() && class.count <= 32768,
                "provided size class counts must be a power of two no larger than 32768"
            );
        }
        // Start the class on an offset aligned to its own slot size, capped
        // at one page, so slot `i` sits at `base_offset + i * size`, a
        // multiple of `min(size, BUFFER_MAX_ALIGN)`.
        let size = u64::from(class.size);
        let align = u64::from(class.size.min(BUFFER_MAX_ALIGN));
        let base_offset = base.div_ceil(align) * align;
        let total = base_offset + size * u64::from(class.count);
        assert!(
            total <= u64::from(u32::MAX),
            "buffer slab exceeds the 4 GiB offset range"
        );
        layouts.push(SizeClassLayout {
            size: class.size,
            count: class.count,
            base_offset: base_offset as u32,
            total,
        });
        base = total;
    }
    layouts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_for_picks_smallest_fitting_class() {
        let classes = [
            SizeClass { size: 64, count: 1 },
            SizeClass {
                size: 256,
                count: 1,
            },
            SizeClass {
                size: 1024,
                count: 1,
            },
        ];
        assert_eq!(class_for(&classes, 1), Some(0));
        assert_eq!(class_for(&classes, 64), Some(0));
        assert_eq!(class_for(&classes, 65), Some(1));
        assert_eq!(class_for(&classes, 256), Some(1));
        assert_eq!(class_for(&classes, 257), Some(2));
        assert_eq!(class_for(&classes, 1024), Some(2));
        assert_eq!(class_for(&classes, 1025), None);
    }

    #[test]
    fn packed_bid_roundtrip() {
        for class in 0..256u32 {
            for local in [0u32, 1, BID_LOCAL_MASK] {
                let bid = pack_bid(class, local);
                assert_eq!(bid_class(bid), class);
                assert_eq!(bid_local(bid), local);
            }
        }
        assert_eq!(pack_bid(0, 1), 1);
        assert_eq!(pack_bid(1, 0), 1 << 24);
    }

    #[test]
    fn layout_classes_sorts_and_computes_offsets() {
        let classes = [
            SizeClass {
                size: 1024,
                count: 2,
            },
            SizeClass { size: 64, count: 4 },
            SizeClass {
                size: 256,
                count: 3,
            },
        ];
        let layouts = layout_classes(&classes, false);
        let sizes: Vec<u32> = layouts.iter().map(|l| l.size).collect();
        assert_eq!(sizes, vec![64, 256, 1024]);
        assert_eq!(layouts[0].base_offset, 0);
        assert_eq!(layouts[0].total, 64 * 4);
        assert_eq!(layouts[1].base_offset, 64 * 4);
        assert_eq!(layouts[1].total, 64 * 4 + 256 * 3);
        assert_eq!(layouts[2].base_offset, 64 * 4 + 256 * 3);
    }

    #[test]
    fn layout_classes_aligns_each_class_to_its_size() {
        // Non-multiple sizes: back-to-back layout would misalign the classes.
        let classes = [
            SizeClass { size: 3, count: 2 },
            SizeClass { size: 5, count: 2 },
            SizeClass { size: 4, count: 1 },
        ];
        let layouts = layout_classes(&classes, false);
        for layout in &layouts {
            assert_eq!(
                layout.base_offset % layout.size,
                0,
                "class {:?} must start on an offset aligned to its size",
                layout
            );
        }
        // No class overlaps the previous one's slots.
        for pair in layouts.windows(2) {
            assert!(u64::from(pair[1].base_offset) >= pair[0].total);
        }
        // 3x2 = 6, then 6 aligned up to 4 is 8; 8 + 4x1 = 12, aligned to 5
        // gives 15; 15 + 5x2 = 25.
        assert_eq!(layouts[0].base_offset, 0);
        assert_eq!(layouts[0].total, 6);
        assert_eq!(layouts[1].base_offset, 8);
        assert_eq!(layouts[1].total, 12);
        assert_eq!(layouts[2].base_offset, 15);
        assert_eq!(layouts[2].total, 25);
    }

    #[test]
    fn layout_classes_rejects_empty() {
        assert!(std::panic::catch_unwind(|| layout_classes(&[], false)).is_err());
    }

    #[test]
    fn layout_classes_rejects_duplicate_sizes() {
        let classes = [SizeClass { size: 64, count: 1 }, SizeClass {
            size: 64,
            count: 2,
        }];
        assert!(std::panic::catch_unwind(|| layout_classes(&classes, false)).is_err());
    }

    #[test]
    fn layout_classes_rejects_non_power_of_two_provided_counts() {
        let classes = [SizeClass { size: 64, count: 3 }];
        assert!(std::panic::catch_unwind(|| layout_classes(&classes, true)).is_err());
        assert!(layout_classes(&classes, false).len() == 1);
    }
}
