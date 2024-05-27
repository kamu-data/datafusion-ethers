// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.25;

contract Contract {
    event Foo(address indexed addr, uint64 indexed id);
    event Bar(address indexed addr, string str);

    constructor() {}

    function emitLogs() public {
        emit Foo(msg.sender, 123);
        emit Bar(address(this), "a-bar");
    }
}
