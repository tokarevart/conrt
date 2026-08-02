//! Buffer size levels and the packed-bid layout shared by the read and write
//! buffer pools.
//!
//! Each direction's pool memory is split into a user-defined set of levels; a
//! level is a run of equal-sized slots. A [`ReadBuffer`]/[`WriteBuffer`]
//! smart pointer identifies its slot with a packed `bid`: the top byte is the
//! level index and the low 24 bits are the slot's id within that level.

/// A run of equal-sized buffer slots.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Level {
    /// Slot size in bytes.
    pub size: u32,
    /// Number of slots in the level.
    pub count: u32,
}

/// The default level table, used by [`crate::runtime::RuntimeParams::default`].
pub const DEFAULT_LEVELS: [Level; 6] = [
    Level {
        size: 64,
        count: 2048,
    },
    Level {
        size: 256,
        count: 2048,
    },
    Level {
        size: 1024,
        count: 512,
    },
    Level {
        size: 4096,
        count: 128,
    },
    Level {
        size: 16384,
        count: 16,
    },
    Level {
        size: 65536,
        count: 2,
    },
];

pub(crate) const BID_LEVEL_SHIFT: u32 = 24;
pub(crate) const BID_LOCAL_MASK: u32 = 0x00FF_FFFF;

/// Packs a level index and a local slot id into the global `bid` carried by a
/// buffer smart pointer.
pub(crate) fn pack_bid(level: u32, local: u32) -> u32 {
    debug_assert!(level < 256);
    debug_assert!(local <= BID_LOCAL_MASK);
    (level << BID_LEVEL_SHIFT) | local
}

/// The level index stored in the top byte of a packed `bid`.
pub(crate) fn bid_level(bid: u32) -> u32 {
    bid >> BID_LEVEL_SHIFT
}

/// The slot's id within its level, stored in the low 24 bits of a packed `bid`.
pub(crate) fn bid_local(bid: u32) -> u32 {
    bid & BID_LOCAL_MASK
}

/// Returns the index of the smallest level whose slot size is at least `size`.
///
/// `levels` must be sorted by ascending size.
pub(crate) fn level_for(levels: &[Level], size: usize) -> Option<usize> {
    levels.iter().position(|level| level.size as usize >= size)
}

/// The validated layout of one level inside a direction's shared slab.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LevelLayout {
    pub size: u32,
    pub count: u32,
    /// Byte offset of the level's first slot within the shared slab.
    pub base_offset: u32,
    /// Cumulative slab bytes through the end of this level.
    pub total: u64,
}

/// Sorts `levels` by ascending size, validates the layout, and returns the
/// per-level base offsets and cumulative totals. The returned order is the
/// level-index order used by the packed `bid` and by `buf_group`.
///
/// With `power_of_two_counts`, every `count` must be a power of two no larger
/// than `32768`, matching the kernel's provided-buffer-ring limit.
pub(crate) fn layout_levels(levels: &[Level], power_of_two_counts: bool) -> Vec<LevelLayout> {
    assert!(!levels.is_empty(), "at least one buffer level is required");
    assert!(
        levels.len() <= 256,
        "no more than 256 buffer levels are supported (the packed bid has one level byte)"
    );

    let mut sorted: Vec<Level> = levels.to_vec();
    sorted.sort_by_key(|level| level.size);

    for pair in sorted.windows(2) {
        assert!(
            pair[0].size < pair[1].size,
            "buffer level sizes must be strictly increasing"
        );
    }

    let mut layouts = Vec::with_capacity(sorted.len());
    let mut base: u64 = 0;
    for level in &sorted {
        assert!(level.size > 0, "buffer level size must be nonzero");
        assert!(level.count > 0, "buffer level count must be nonzero");
        assert!(
            u64::from(level.count) <= u64::from(BID_LOCAL_MASK),
            "buffer level count exceeds the packed-bid local range"
        );
        if power_of_two_counts {
            assert!(
                level.count.is_power_of_two() && level.count <= 32768,
                "read buffer level counts must be a power of two no larger than 32768"
            );
        }
        let total = base + u64::from(level.size) * u64::from(level.count);
        assert!(
            total <= u64::from(u32::MAX),
            "buffer slab exceeds the 4 GiB offset range"
        );
        layouts.push(LevelLayout {
            size: level.size,
            count: level.count,
            base_offset: base as u32,
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
    fn level_for_picks_smallest_fitting_level() {
        let levels = [
            Level { size: 64, count: 1 },
            Level {
                size: 256,
                count: 1,
            },
            Level {
                size: 1024,
                count: 1,
            },
        ];
        assert_eq!(level_for(&levels, 1), Some(0));
        assert_eq!(level_for(&levels, 64), Some(0));
        assert_eq!(level_for(&levels, 65), Some(1));
        assert_eq!(level_for(&levels, 256), Some(1));
        assert_eq!(level_for(&levels, 257), Some(2));
        assert_eq!(level_for(&levels, 1024), Some(2));
        assert_eq!(level_for(&levels, 1025), None);
    }

    #[test]
    fn packed_bid_roundtrip() {
        for level in 0..256u32 {
            for local in [0u32, 1, BID_LOCAL_MASK] {
                let bid = pack_bid(level, local);
                assert_eq!(bid_level(bid), level);
                assert_eq!(bid_local(bid), local);
            }
        }
        assert_eq!(pack_bid(0, 1), 1);
        assert_eq!(pack_bid(1, 0), 1 << 24);
    }

    #[test]
    fn layout_levels_sorts_and_computes_offsets() {
        let levels = [
            Level {
                size: 1024,
                count: 2,
            },
            Level { size: 64, count: 4 },
            Level {
                size: 256,
                count: 3,
            },
        ];
        let layouts = layout_levels(&levels, false);
        let sizes: Vec<u32> = layouts.iter().map(|l| l.size).collect();
        assert_eq!(sizes, vec![64, 256, 1024]);
        assert_eq!(layouts[0].base_offset, 0);
        assert_eq!(layouts[0].total, 64 * 4);
        assert_eq!(layouts[1].base_offset, 64 * 4);
        assert_eq!(layouts[1].total, 64 * 4 + 256 * 3);
        assert_eq!(layouts[2].base_offset, 64 * 4 + 256 * 3);
    }

    #[test]
    fn layout_levels_rejects_empty() {
        assert!(std::panic::catch_unwind(|| layout_levels(&[], false)).is_err());
    }

    #[test]
    fn layout_levels_rejects_duplicate_sizes() {
        let levels = [Level { size: 64, count: 1 }, Level { size: 64, count: 2 }];
        assert!(std::panic::catch_unwind(|| layout_levels(&levels, false)).is_err());
    }

    #[test]
    fn layout_levels_rejects_non_power_of_two_read_counts() {
        let levels = [Level { size: 64, count: 3 }];
        assert!(std::panic::catch_unwind(|| layout_levels(&levels, true)).is_err());
        assert!(layout_levels(&levels, false).len() == 1);
    }
}
