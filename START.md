# Capella queue
## Install
You can build the project 
### Build
#### Linux or Mac
#### Windows

## Init empty Capella cluster (CAP server)
### 1. Config storage config
Copy and fill the file [config/stor.config.json](config/stor.config.json) in to the work path (`work_path`).  
The file contains storage mount points  
``` json
{
    "mounts": {
        "default": { // "default" is alias of mount point
            "provider": "file_dbl_save", // type of storage provider
            "home_path": "tmp/", // the path where files will be stored
            "params": {}
        },
        "meta": {
            "provider": "file_dbl_save",
            "home_path": "tmp/meta/",
            "params": {}
        },
        "fast": {
            "provider": "file_dbl_save",
            "home_path": "tmp/fast/",
            "params": {}
        },
        "compress": {
            "provider": "file_dbl_save_gzip",
            "home_path": "tmp/zip/",

            "compress_alg" : "gzip", // compress algorithm
            "file_extention" : ".gz", // file extention

            "params": {}
        }
    }
}
```

### 2. Run encrypt_data_generator
- Windows: `encrypt_data_generator.exe -cfge "encrypt.json"`  
- Linux|mac `./encrypt_data_generator -cfge "encrypt.json"`  
This tool generates `encrypt.json`. The file contains server encrypt key for passwords. Copy this file in to the work path.  
Example:  
``` json
{
  "enc_alg": "aes",
  "enc_key": "4S5H/Y/LjNUTBB5Bt+a4Xz3SjrTZ9ZEk2AzYW6jp478=",
  "dec_alg": "aes",
  "dec_key": "4S5H/Y/LjNUTBB5Bt+a4Xz3SjrTZ9ZEk2AzYW6jp478="
}
```

### 3. Copy empty cluster files
Copy files with empty cluster to the `default` path (Described in [config/stor.config.json](config/stor.config.json)).
- [config/cluster.json](config/cluster.json)
- [config/basic_auth.json](config/basic_auth.json)
- [config/authorization.json](config/authorization.json)

### 4. RUN Cluster
Without tls:  
- Linux|mac `./capserver -cfg "work_path/stor.config.json" -cfge "work_path/encrypt.json" -abfn "basic_auth.json" -arfn "authorization.json" -log_level info`
- Windows: `capserver.exe -cfg "work_path/stor.config.json" -cfge "work_path/encrypt.json" -abfn "basic_auth.json" -arfn "authorization.json" -log_level info`

With tls:  
- Linux|mac `./capserver -cfg "work_path/stor.config.json" -cfge "work_path/encrypt.json" -abfn "basic_auth.json" -arfn "authorization.json" -tls_key "app/key.pem" -tls_cert "app/cert.pem" -log_level info`
- Windows: `capserver.exe -cfg "work_path/stor.config.json" -cfge "work_path/encrypt.json" -abfn "basic_auth.json" -arfn "authorization.json" -tls_key "app/key.pem" -tls_cert "app/cert.pem" -log_level info`

##### !!!Congratulations!!!
Your Capella cluster is up and running now.  

## Make the initial setup
### 1. Prepre connection config file
Copy and fill the file [config/connection.json](config/connection.json) in to the work path of admin tools.  
``` json
{
    "connections": {
        "admin": {
            "connection": {
                "server": "http://localhost:8676",
                "ignore_ssql_validation": true,
                "query_wait": 5000000000,
                "max_conn": 5,
                "max_idle_duration": 5
            },
            "auth_type": "basic",
            "__auth_info_is_base64_from__": "\"Pa$$w0rd\"",
            "auth_info": "IlBhJCR3MHJkIg==", // set there your password
            "user_name": "admin",
            "prefer_content_type": "gzip",
            "send_content_type": "gzip",
            "replace_name_force": false
        },
        "empty": {
            "connection": {
                "server": "http://localhost:8676",
                "ignore_ssql_validation": true,
                "query_wait": 5000000000,
                "max_conn": 5,
                "max_idle_duration": 5
            },
            "auth_type": "",
            "auth_info": null,
            "user_name": "",
            "prefer_content_type": "gzip",
            "send_content_type": "gzip",
            "replace_name_force": false
        }
    }
}
```

### 2. Create `admin` user and disable empty user 
`$ ./capsec` for windows: `capsec.exe`  

``` bash
$ ./capsec -cn empty -cmd create_user -un admin
Enter your pwd: Pa$$w0rd
OK
$ ./capsec -cn empty -cmd enable_user -un admin
OK
$ ./capsec -cn empty -cmd add_sec_user -un admin -is_admin
OK
$ ./capsec -cmd disable_user -un ""
OK
$ ./capsec -cmd set_is_admin_sec_user
OK
$ ./capsec -cmd get_users
[
  {
    "name": "admin",
    "is_enable": true
  },
  {
    "name": "",
    "is_enable": false
  }
]
```

Commands `$ ./capsec -cmd disable_user` and `$ ./capsec -cmd disable_user -un ""` are equal  

## Let's create some objects in the cluster 
`$ ./capsec` for windows: `capsec.exe`  

### 1. Create `example_tech_user` 
``` bash
$ ./capsec -cmd create_user -un example_tech_user
Enter your pwd: Pa$$w0rd
OK
$ ./capsec -cmd enable_user -un example_tech_user
OK
$ ./capsec -cmd add_sec_user -un example_tech_user
OK
$ ./capsec -cmd set_rule -un example_tech_user -a -ot CLUSTER_SELF -act GET_QUEUE -on "*"
OK
$ ./capsec -cmd set_rule -un example_tech_user -a -ot CLUSTER_SELF -act GET_EXT_CLUSTER -on "*"
OK
$ ./capsec -cmd get_users
[
  {
    "name": "example_tech_user",
    "is_enable": true
  },
  {
    "name": "",
    "is_enable": false
  },
  {
    "name": "admin",
    "is_enable": true
  }
]
$ ./capsec -cmd get_sec_users
{
  "users": {
    "": {
      "name": "",
      "rule": null
    },
    "admin": {
      "name": "admin",
      "is_admin": true,
      "rule": {}
    },
    "example_tech_user": {
      "name": "example_tech_user",
      "rule": {
        "CLUSTER_SELF": {
          "GET_EXT_CLUSTER": {
            "*": true
          },
          "GET_QUEUE": {
            "*": true
          }
        }
      }
    }
  }
}
```

### 2. Create queue
``` bash
$ ./cap -cmd q_add -pf new_queue.json
OK
$ ./cap -cmd q_add -pf new_queue2.json
OK
```
### 3. Create link on external cluster (loop link in examples)
``` bash
$ ./cap -cmd exc_add -pf new_external_cluster.json
OK
```

### 4. Add copy handlers
``` bash
$ ./cap -cmd h_add -pf new_copy_handler.json
OK
$ ./cap -cmd h_add -pf new_copy_handler2.json
OK
$ ./cap -cmd h_list
[
  "example_queue_to_queue_2_copy_unique",
  "example_queue_2_to_queue_copy_unique"
]
$ ./cap -cmd h_start -name example_queue_to_queue_2_copy_unique
OK
$ ./cap -cmd h_start -name example_queue_2_to_queue_copy_unique
OK
$ ./cap -cmd h_last_error -name example_queue_2_to_queue_copy_unique
No errors
$ ./cap -cmd h_last_complete -name example_queue_2_to_queue_copy_unique
2021-06-19 06:24:01
```


### 5. Add other handlers
``` bash
$ ./cap -cmd h_add -pf new_delete_handler.json
$ ./cap -cmd h_add -pf new_mark_handler.json
$ ./cap -cmd h_add -pf new_regularly_save_handler.json
$ ./cap -cmd h_add -pf new_regularly_save_handler2.json
$ ./cap -cmd h_add -pf new_unload_handler.json

$ ./cap -cmd h_list
[
  "example_queue_regularly_save",
  "example_queue_regularly_save2",
  "example_queue_unload",
  "example_queue_2_to_queue_copy_unique",
  "example_queue_to_queue_2_copy_unique",
  "example_queue_delete",
  "example_queue_mark"
]

$ ./cap -cmd h_start -name example_queue_regularly_save
OK
$ ./cap -cmd h_start -name example_queue_regularly_save2
OK
$ ./cap -cmd h_start -name example_queue_unload
OK
$ ./cap -cmd h_start -name example_queue_delete
OK
$ ./cap -cmd h_start -name example_queue_mark
OK
```

### 6. Send test msg and get them
```
$ ./cap -cmd q_au -name example_queue -pf new_messages.json -save_mode 2
[
  1624075947165280000,
  1624075947165280001
]
$ ./cap -cmd q_au -name example_queue2 -pf new_messages2.json -save_mode 2
[
  1624075993591360000,
  1624075947169800002,
  1624075993591360001
]
$ ./cap -cmd q_get -name example_queue -qty 10 -id 0
[
  {
    "id": 1624075947165280000,
    "eid": 99,
    "s_dt": 1624075241,
    "dt": "2021-06-19T07:12:27.16528+03:00",
    "msg": "SGVsbG8gd29ybGQh",
    "src": "src1",
    "is_saved": true,
    "sg": 5
  },
  {
    "id": 1624075947165280001,
    "eid": 100,
    "s_dt": 1624075241,
    "dt": "2021-06-19T07:12:27.165283+03:00",
    "msg": "SGVsbG8gd29ybGQhIFYy",
    "src": "src1",
    "is_saved": true,
    "sg": 5
  },
  {
    "id": 1624075993603880001,
    "eid": 99,
    "s_dt": 1624075241,
    "dt": "2021-06-19T07:13:13.603887+03:00",
    "msg": "SGVsbG8gd29ybGQh",
    "src": "src2",
    "is_saved": true,
    "sg": 5
  },
  {
    "id": 1624075993603890000,
    "eid": 100,
    "s_dt": 1624075241,
    "dt": "2021-06-19T07:13:13.603894+03:00",
    "msg": "SGVsbG8gd29ybGQhIFYy",
    "src": "src3",
    "is_saved": true,
    "sg": 5
  }
]
```