use ethers::prelude::*;

///////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) trait FilterExt {
    fn union(self, from: BlockNumber, to: BlockNumber) -> Self;
}

impl FilterExt for Filter {
    fn union(mut self, from: BlockNumber, to: BlockNumber) -> Self {
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

pub(crate) trait BlockNumberExt {
    fn min(self, other: Self) -> Self;
    fn max(self, other: Self) -> Self;
}

impl BlockNumberExt for BlockNumber {
    fn min(self: BlockNumber, other: BlockNumber) -> BlockNumber {
        let a = self;
        let b = other;

        match (a, b) {
            (BlockNumber::Number(a), BlockNumber::Number(b)) => {
                BlockNumber::Number(u64::min(a.as_u64(), b.as_u64()).into())
            }
            (BlockNumber::Number(_), _) => a,
            (BlockNumber::Latest, _) => b,
            (BlockNumber::Safe, BlockNumber::Latest) => BlockNumber::Safe,
            (BlockNumber::Safe, _) => b,
            (BlockNumber::Finalized, BlockNumber::Latest | BlockNumber::Safe) => {
                BlockNumber::Finalized
            }
            (BlockNumber::Finalized, _) => b,

            _ => unreachable!(),
        }
    }

    fn max(self: BlockNumber, other: BlockNumber) -> BlockNumber {
        let a = self;
        let b = other;

        match (a, b) {
            (BlockNumber::Earliest, _) => b,
            (_, BlockNumber::Earliest) => a,
            (BlockNumber::Number(a), BlockNumber::Number(b)) => {
                BlockNumber::Number(u64::max(a.as_u64(), b.as_u64()).into())
            }
            _ => unreachable!(),
        }
    }
}
