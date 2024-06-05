use std::{path::PathBuf, sync::Arc};

use alloy_core::hex::ToHexExt;
use ethers::{prelude::*, utils::AnvilInstance};
use tokio::sync::{Mutex, MutexGuard};

///////////////////////////////////////////////////////////////////////////////////////////////////

abigen!(
    TestContract,
    "tests/contracts/out/Contract.sol/Contract.json"
);

///////////////////////////////////////////////////////////////////////////////////////////////////

type StateT = Option<(Arc<AnvilInstance>, Arc<Provider<Http>>)>;

static TEST_CHAIN_STATE: Mutex<StateT> = Mutex::const_new(None);

///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestChain<'a> {
    pub anvil: Arc<AnvilInstance>,
    pub rpc_client: Arc<Provider<Http>>,
    // Anvil does not like concurrent access so we have to serialize
    // all tests that are accessing it
    #[allow(dead_code)]
    guard: MutexGuard<'a, StateT>,
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn get_test_chain() -> TestChain<'static> {
    let mut state_guard = TEST_CHAIN_STATE.lock().await;

    if state_guard.is_none() {
        let anvil = ethers::core::utils::Anvil::new().spawn();
        let rpc_client = Arc::new(Provider::<Http>::connect(&anvil.endpoint()).await);

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

        let contract_1 = TestContract::new(contract_1_address, rpc_client.clone());
        let contract_2 = TestContract::new(contract_2_address, rpc_client.clone());

        contract_1
            .emit_logs()
            .from(admin_address)
            .send()
            .await
            .unwrap();

        contract_2
            .emit_logs()
            .from(admin_address)
            .send()
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
