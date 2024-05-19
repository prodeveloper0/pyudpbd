# PyUDPBD Server

----

Python implementation of the UDP-based block device server for the PlayStation 2 with utility scripts make easy to use.

**UDPBD** is developed by [@rickgaiser](https://github.com/rickgaiser)


## Requirements
* Python 3.10 or higher
* Network connection (recommended direct connection to the PS2)
* Open PS2 Loader supporting **UDPBD** (v1.2.0 beta 1973 built by @El_isra)

## Usages
### Standalone

---
Just type scripts with arguments
```shell
sudo python3 server.py --path <<path>>
```
Path can be used with block device or mount point

You can block writing with `--read-only` option
```shell
sudo python3 server.py --path <<path>> --read-only
```

### With monitor

---
**PyUDPBD** provides block device monitor to run the server automatically

```shell
sudo python3 monitor.py --path <<path>> --period <<period:10>> --flag <<flag:pyudpbd>>
```

the monitor will find a flag file each block devices periodically and run the server with the path of the block device first of iteration

It is useful with SBC (like Raspberry Pi) to serve block device you plugged in

#### PM2 integration with monitor

---
PM2 is a process manager for Node.js applications, but it can run Python scripts as well and good for running the monitor when server is started

```shell
sudo pm2 start monitor.py --interpreter python3 --name pyudpbd-monitor
sudo pm2 save
sudo pm2 startup
```
⚠️ **PM2** must be started with `sudo` to run the monitor with root permission required to access block devices. ⚠️ 

## Limitations
* Unstable game playing with router connection (may need to change the protocol to send reading **ACK**)

  **Highly recommended to use direct connection to the PS2**

## TODO
- [ ] Support block device caching
- [ ] Support multiple clients (required to change both side of server and driver)
- [ ] Sending reading ACK (required to change both side of server and driver)