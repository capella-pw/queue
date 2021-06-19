

### Create `admin` user and disable empty user 
``` bash
$ ./capsec -cn empty -cmd create_user -un admin
Enter your pwd: Pa$$w0rd
OK
$ ./capsec -cn empty -cmd enable_user -un admin
OK
$ ./capsec -cn empty -cmd add_sec_user -un admin -is_admin true
OK
$ ./capsec -cmd disable_user -un ""
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

### Create `example_tech_user` 
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
      "is_admin": true,
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

### Create queue
``` bash
$ ./cap -cmd q_add -pf new_queue.json
OK
$ ./cap -cmd q_add -pf new_queue2.json
OK
```
### Create link on external cluster (loop link in examples)
``` bash
$ ./cap -cmd exc_add -pf new_external_cluster.json
OK
```

### Add copy handlers
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


### Add other handlers
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

### Send test msg and get them
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