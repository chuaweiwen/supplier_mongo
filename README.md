# CS4224 Project - MongoDB

## Introduction

This is a project for the module CS4224 Distributed Databases offered by the National University of Singapore (NUS). The project uses Mongo Database (MongoDB) to simulate a warehouse supplier application, and to measure the performance of the application.

## Pre-requisite

The following application is required to compile and run the application:

- [MongoDB 4.0](https://www.mongodb.com/download-center/community) and above
- [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and above
- [Maven 3.5.4](https://maven.apache.org/download.cgi) and above
- [Datastax 3.3.9](https://academy.datastax.com/quick-downloads) and above

## Configuration

The following are the instructions to configure MongoDB for the application. We assume that there will be 5 servers and we are using UNIX.

### Installation of MongoDB

1. To access a server, use `ssh [host]`. Example: `ssh 192.168.48.239`
2. Place the files in the MongoDB 4.0 onto all machines into the directory `/temp/mongodb`. The `bin` folder should be in the `mongodb` folder.
3. Create two folders `data` and `log` in `/temp/mongodb` for each server.
4. Inside the `data` folder for each server, create five folders: `shard0`, `shard1`, `shard2`, `shard3`, `shard4`
5. Choose the first three servers as the configuration server - in these three servers, create a folder `cfgsvr` in the `data` folder.

### Setting up configuration servers

We will setting up the configuration servers at port `21000`.

1. Go to one of the configuration servers as mentioned in the previous section, go to the directory `/temp/mongodb`.
2. Run the following command (replace `[host]` with the IP address of the current server):
```
bin/mongod --fork --configsvr --directoryperdb --pidfilepath pid --dbpath data/cfgsvr --logpath log/cfgsvr.log --replSet cfg --port 21000 --bind_ip [host]
```
- Example:
```
bin/mongod --fork --configsvr --directoryperdb --pidfilepath pid --dbpath data/cfgsvr --logpath log/cfgsvr.log --replSet cfg --port 21000 --bind_ip xcnd20.comp.nus.edu.sg
```

3. Repeat Step 1 and 2 with the other two servers.
4. Go to one of the three servers and move to the `/temp/mongodb` folder and run the following command to access the Mongo shell:
- `bin/mongo --host [host]:21000`
- Example: `bin/mongo --host xcnd20.comp.nus.edu.sg:21000`

5. In the Mongo Shell, enter the following command:
```
rs.initiate(
    {
        _id: "cfg",
        configsvr: true,
        members: [
             { _id : 0, host : "x:21000" },
             { _id : 1, host : "y:21000" },
             { _id : 2, host : "z:21000" }
        ]
    }
)
```
- where `x`, `y` and `z` are the host names for your configuration servers. For example:
```
rs.initiate(
    {
        _id: "cfg",
        configsvr: true,
        members: [
            { _id : 0, host : "xcnd20.comp.nus.edu.sg:21000" },
            { _id : 1, host : "xcnd21.comp.nus.edu.sg:21000" },
            { _id : 2, host : "xcnd22.comp.nus.edu.sg:21000" }
        ]
    }
)
```

### Setting up replica sets

In this example we will be setting up five replica sets.

1. You should set up the following replica set at these servers:
- `shard0` at server 1, 2, 3 at port `21001`
- `shard1` at server 2, 3, 4 at port `21002`
- `shard2` at server 3, 4, 5 at port `21003`
- `shard3` at server 4, 5, 1 at port `21004`
- `shard4` at server 5, 1, 2 at port `21005`.
- Example: For server 1, you should set up `shard0`, `shard3` and `shard4`.

2. Go to the first server and run the following command:
```
bin/mongod --fork --shardsvr --directoryperdb --pidfilepath pidshard[x] --dbpath data/shard[x] --logpath log/shard[x].log --replSet shard[x] --port [port] --bind_ip [host]
```

- Example to set up `shard0`, `shard4`, and `shard5` at server 1 `xcnd20.comp.nus.edu.sg` at port `21001`:
```aidl
bin/mongod --fork --shardsvr --directoryperdb --pidfilepath pidshard0 --dbpath data/shard0 --logpath log/shard0.log --replSet shard0 --port 21001 --bind_ip xcnd20.comp.nus.edu.sg
bin/mongod --fork --shardsvr --directoryperdb --pidfilepath pidshard3 --dbpath data/shard3 --logpath log/shard3.log --replSet shard3 --port 21004 --bind_ip xcnd20.comp.nus.edu.sg
bin/mongod --fork --shardsvr --directoryperdb --pidfilepath pidshard4 --dbpath data/shard4 --logpath log/shard4.log --replSet shard4 --port 21005 --bind_ip xcnd20.comp.nus.edu.sg
```

3. Repeat Step 2 for the other four servers with the corresponding shard number and port number.

4. Go to the first server where `shard0` is located and go to `/temp/mongodb` and run the command to access the Mongo Shell for `shard0`:
```
bin/mongo --host x:21001
```
- where `x` is the host name. Example: `bin/mongo --host xcnd20.comp.nus.edu.sg:21001`

5. In the Mongo Shell, enter the following command:
```aidl
rs.initiate(
    {
        _id: "shard0",
        members: [
            { _id : 0, host : "x:21001" },
            { _id : 1, host : "y:21001" },
            { _id : 2, host : "z:21001" },
        ]
    }
)
```
- where `x`, `y` and `z` are the host names belonging to `shard0`.
- For example for `shard0`:
```aidl
rs.initiate(
    {
        _id: "shard0",
        members: [
            { _id : 0, host : "xcnd20.comp.nus.edu.sg:21001" },
            { _id : 1, host : "xcnd21.comp.nus.edu.sg:21001" },
            { _id : 2, host : "xcnd22.comp.nus.edu.sg:21001" },
        ]
    }
)
```

6. Repeat Step 4 and 5 on the other four servers for other four shards and its corresponding port number.

### Setting up mongos instance

We will be setting up `mongos` at each server at port `21100`.

1. Go to the first server to go to `temp/mongodb`.
2. Enter the following command:
```
bin/mongos --fork --pidfilepath pidmongos --logpath log/mongos_[n].log --configdb cfg/x:21000,y:21000,z:21000 --port 21100 --bind_ip [host]
```
- where `[n]` is the server number minus one (e.g. first server is 0, second is 1 etc.), `x` and `y` and `z` are the three host names of the configuration servers, and `[host]` is the host name of the current server.
- Example for the first server `xcnd20.comp.nus.edu.sg`:
```aidl
bin/mongos --fork --pidfilepath pidmongos --logpath log/mongos_0.log --configdb cfg/xcnd20.comp.nus.edu.sg:21000,xcnd21.comp.nus.edu.sg:21000,xcnd22.comp.nus.edu.sg:21000 --port 21100 --bind_ip xcnd20.comp.nus.edu.sg
```

3. Repeat Step 1 to 2 on the other four servers.

### Adding shards

1. Go to the first server to go to `temp/mongodb`.
2. Enter the following command to access `mongos` in Mongo Shell:
```aidl
bin/mongo --host [host]:21100
```
- Example:
```aidl
bin/mongo --host xcnd20.comp.nus.edu.sg:21100
```
3. Add the `shard0` using the command in the Mongo Shell:
```aidl
sh.addShard( "shard0/x:21001" )
sh.addShard( "shard0/y:21001" )
sh.addShard( "shard0/z:21001" )
```
- where `x`, `y` and `z` are the host names belonging to `shard0`.
- For example for `shard0`:
```aidl
sh.addShard( "shard0/xcnd20.comp.nus.edu.sg:21001" )
sh.addShard( "shard0/xcnd21.comp.nus.edu.sg:21001" )
sh.addShard( "shard0/xcnd22.comp.nus.edu.sg:21001" )
```

4. Repeat Step 2 to 3 on the other four servers with the corresponding shard number, host names, and port number.

### Importing data

In this example, we will be importing data from a folder `raw_data` into the database.

1. Move the folder `raw_data` containing all the data files to `/temp/mongodb` on the first server.
2. Run these commands (make sure you are at the directory `/temp/mongodb`) to import the data:
```aidl
bin/mongoimport -d supplier -c warehouse --type csv --file raw_data/warehouse.csv --fields w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,w_ytd

bin/mongoimport -d supplier -c district --type csv --file raw_data/district.csv --fields d_w_id,d_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,d_ytd,d_next_o_id

bin/mongoimport -d supplier -c customer --type csv --file raw_data/customer.csv --fields c_w_id,c_d_id,c_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data

bin/mongoimport -d supplier -c orders --type csv --file raw_data/order.csv --fields o_w_id,o_d_id,o_id,o_c_id,o_carrier_id,o_ol_cnt,o_all_local,o_entry_d

bin/mongoimport -d supplier -c item --type csv --file raw_data/item.csv --fields i_id,i_name,i_price,i_im_id,i_data

bin/mongoimport -d supplier -c orderline --type csv --file raw_data/order-line.csv --fields ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info

bin/mongoimport -d supplier -c stock --type csv --file raw_data/stock.csv --fields s_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_data
```
- Note: The name after `-d` is the name of the database. In this example, we use `supplier`.
3. Leave the server and go to the root directory where the project files are located.
4. Compile the project via `mvn clean dependency:copy-dependencies package`.
- If the `mvn` command is not available, type the command: `export PATH=<file location of Apache Maven>:$PATH` (e.g. `export PATH=/home/stuproj/cs4224e/apache-maven-3.5.4/bin:$PATH`), then run the `mvn` command again.
5. Copy `config.env.example` and place it on the same directory and rename it to `config.env`.
6. Change the values in `config.env` and change the `HOST` value to the hostname of the first server, and `PORT` to `21100` (i.e. port number for the `mongos` instance). Change the value for the `DATABASE` to the name of your database. For example:
```aidl
HOST=xcnd20.comp.nus.edu.sg
PORT=21100
DATABASE=supplier
CONSISTENCY_LEVEL=local
NUMBER_OF_TRANSACTIONS=10
```
7. Run `java -Xms4096m -Xmx4096m -cp target/*:target/dependency/*:. main.java.TableUpdate` to add indexes and update the schemas for the database.

### Enable Sharding

1. After importing the data, go to the first server and go to `/temp/mongo`.
2. Connect to `mongos` using the command:
```aidl
bin/mongo --host [host]:21100
```
- Example:
```aidl
bin/mongo --host xcnd20.comp.nus.edu.sg:21100
```
3. In the Mongo Shell, enter the following commands:
```aidl
sh.enableSharding("supplier")
sh.shardCollection("supplier.warehouse", {w_id: 1})
sh.shardCollection("supplier.customer", { c_w_id: 1, c_d_id: 1, c_id: 1 })
sh.shardCollection("supplier.orders", { o_w_id: 1, o_d_id: 1, o_id: 1 })
sh.shardCollection("supplier.orderline", { ol_w_id: 1, ol_d_id: 1, ol_o_id: 1 })
sh.shardCollection("supplier.stock", { s_w_id : 1, s_i_id: 1 })
```
- Note: The name of the database used in this example is `supplier`. If you use other names, change it accordingly.

4. Run the balancer to partition the data:
```aidl
sh.startBalancer()
```

5. Wait until the balancer stops running. To check if the balancer running, run this command:
```aidl
sh.isBalancerRunning()
```

### Performance Measurement

1. Create a folder `xact`, if not available, in the same file level as your program `src` folder.
2. Insert the xact files (in .txt format) into the `xact` folder.
3. Copy `config.env.example` and place it on the same directory and rename it to `config.env`.
4. Edit the values `config.env` according to the experiment you want to run. For example, to run with consistency level `local` with `40` clients:
```aidl
HOST=xcnd20.comp.nus.edu.sg
PORT=21100
DATABASE=supplier
CONSISTENCY_LEVEL=local
NUMBER_OF_TRANSACTIONS=40
```
5. Run `java -Xms4096m -Xmx4096m -cp target/*:target/dependency/*:. main.java.Main` to run the experiment.

