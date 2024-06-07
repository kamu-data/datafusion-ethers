use alloy::rpc::types::eth::{BlockNumberOrTag, Filter, FilterBlockOption};

///////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) trait FilterExt {
    fn union(self, from: BlockNumberOrTag, to: BlockNumberOrTag) -> Self;
}

impl FilterExt for Filter {
    fn union(mut self, from: BlockNumberOrTag, to: BlockNumberOrTag) -> Self {
        self.block_option = self.block_option.union(FilterBlockOption::Range {
            from_block: Some(from),
            to_block: Some(to),
        });
        self
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
pub(crate) trait FilterBlockOptionExt {
    fn union(self, other: Self) -> Self;
}

impl FilterBlockOptionExt for FilterBlockOption {
    fn union(self: FilterBlockOption, other: FilterBlockOption) -> FilterBlockOption {
        let a = self;
        let b = other;

        match (a, b) {
            (FilterBlockOption::AtBlockHash(h_a), FilterBlockOption::AtBlockHash(h_b))
                if h_a == h_b =>
            {
                a
            }
            (
                FilterBlockOption::Range {
                    from_block: Some(from_a),
                    to_block: Some(to_a),
                },
                FilterBlockOption::Range {
                    from_block: Some(from_b),
                    to_block: Some(to_b),
                },
            ) => FilterBlockOption::Range {
                from_block: Some(from_a.max(from_b)),
                to_block: Some(to_a.min(to_b)),
            },
            _ => panic!("Can't create a union of block ranges {a:?} and {b:?}"),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) trait BlockNumberOrTagExt {
    fn min(self, other: Self) -> Self;
    fn max(self, other: Self) -> Self;
}

impl BlockNumberOrTagExt for BlockNumberOrTag {
    fn min(self: BlockNumberOrTag, other: BlockNumberOrTag) -> BlockNumberOrTag {
        let a = self;
        let b = other;

        match (a, b) {
            (BlockNumberOrTag::Number(a), BlockNumberOrTag::Number(b)) => {
                BlockNumberOrTag::Number(u64::min(a, b))
            }
            (BlockNumberOrTag::Number(_), _) => a,
            (BlockNumberOrTag::Latest, _) => b,
            (BlockNumberOrTag::Safe, BlockNumberOrTag::Latest) => BlockNumberOrTag::Safe,
            (BlockNumberOrTag::Safe, _) => b,
            (BlockNumberOrTag::Finalized, BlockNumberOrTag::Latest | BlockNumberOrTag::Safe) => {
                BlockNumberOrTag::Finalized
            }
            (BlockNumberOrTag::Finalized, _) => b,

            _ => unreachable!(),
        }
    }

    fn max(self: BlockNumberOrTag, other: BlockNumberOrTag) -> BlockNumberOrTag {
        let a = self;
        let b = other;

        match (a, b) {
            (BlockNumberOrTag::Earliest, _) => b,
            (_, BlockNumberOrTag::Earliest) => a,
            (BlockNumberOrTag::Number(a), BlockNumberOrTag::Number(b)) => {
                BlockNumberOrTag::Number(u64::max(a, b))
            }
            _ => unreachable!(),
        }
    }
}
