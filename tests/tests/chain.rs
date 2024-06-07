use std::{path::PathBuf, sync::Arc};

use alloy::hex::ToHexExt;
use alloy::sol;
use alloy::{
    node_bindings::{Anvil, AnvilInstance},
    primitives::Address,
    providers::{ProviderBuilder, RootProvider},
    transports::BoxTransport,
};
use tokio::sync::{Mutex, MutexGuard};

///////////////////////////////////////////////////////////////////////////////////////////////////

sol!(
    #[sol(rpc)]
    contract Contract {
        event Foo(address indexed addr, uint64 indexed id);
        event Bar(address indexed addr, string str);

        constructor() {}

        function emitLogs() public {
            emit Foo(msg.sender, 123);
            emit Bar(address(this), "a-bar");
        }
    }
);

///////////////////////////////////////////////////////////////////////////////////////////////////

type StateT = Option<(Arc<AnvilInstance>, RootProvider<BoxTransport>)>;

static TEST_CHAIN_STATE: Mutex<StateT> = Mutex::const_new(None);

///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestChain<'a> {
    pub anvil: Arc<AnvilInstance>,
    pub rpc_client: RootProvider<BoxTransport>,
    // Anvil does not like concurrent access so we serialize
    // all tests that are accessing it
    #[allow(dead_code)]
    guard: MutexGuard<'a, StateT>,
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn get_test_chain() -> TestChain<'static> {
    let mut state_guard = TEST_CHAIN_STATE.lock().await;

    if state_guard.is_none() {
        let anvil = Anvil::new().spawn();
        let rpc_client = ProviderBuilder::new()
            .on_builtin(&anvil.endpoint())
            .await
            .unwrap();

        let contracts_dir = PathBuf::from("tests/contracts");
        let rpc_endpoint = anvil.endpoint();
        let admin_address = anvil.addresses()[0];
        let admin_key = anvil.keys()[0].to_bytes().encode_hex_with_prefix();

        // Deploy 2 contracts
        std::process::Command::new("forge")
            .current_dir(&contracts_dir)
            .args([
                "script",
                "script/Deploy.s.sol",
                "--fork-url",
                rpc_endpoint.as_str(),
                "--private-key",
                admin_key.as_str(),
                "--broadcast",
            ])
            .status()
            .expect("Failed to deploy contracts. Is foundry installed?");

        let contract_1_address: Address = "0x5FbDB2315678afecb367f032d93F642f64180aa3"
            .parse()
            .unwrap();
        let contract_2_address: Address = "0xe7f1725e7734ce288f8367e1bb143e90bb3f0512"
            .parse()
            .unwrap();

        let contract_1 = Contract::new(contract_1_address, rpc_client.clone());
        let contract_2 = Contract::new(contract_2_address, rpc_client.clone());

        contract_1
            .emitLogs()
            .from(admin_address)
            .send()
            .await
            .unwrap()
            .watch()
            .await
            .unwrap();

        contract_2
            .emitLogs()
            .from(admin_address)
            .send()
            .await
            .unwrap()
            .watch()
            .await
            .unwrap();

        *state_guard = Some((Arc::new(anvil), rpc_client));
    }

    let state = state_guard.as_ref().unwrap();

    TestChain {
        anvil: state.0.clone(),
        rpc_client: state.1.clone(),
        guard: state_guard,
    }
}
