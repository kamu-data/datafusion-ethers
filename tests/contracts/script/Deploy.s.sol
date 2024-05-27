// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.25;

import {Contract} from "../src/Contract.sol";
import {BaseScript} from "./Base.s.sol";

contract Deploy is BaseScript {
    function run() public broadcast {
        new Contract();
        new Contract();
    }
}
