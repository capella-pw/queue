# Capella.pw queue
Message queue service.  

## Build prepare and run
### Build:
Go build like:  
`cd server && CGO_ENABLED=0 go build -o ../app/server.app`

### Prepare:
#### Empty cluster.json:
``` json
{
    "name": "",
    "rv_generator_part": 0,
    "object_create_duration": 10000000000 
}
```
#### stor.config.json
Example in `config/stor.config.json`  
```
{
    "mounts": {
        "default": {
            "provider": "file_dbl_save",
            "home_path": "tmp/",
            "params": {}
        },
        "compress": {
            "provider": "file_dbl_save_gzip",
            "home_path": "tmp/zip/",

            "compress_alg" : "gzip",
            "file_extention" : ".gz",

            "params": {}
        },
        ...
```  
mounts - possible file storage
* `"default"`, `"compress"` mount name  
* `provider` - provider of storage  
  * `file` - seve into file  
  * `file_dbl_save` - save file to disk (replace creates new file and rename files)  
  * `file_dbl_save_gzip` - save zip file  
* `home_path` - path on disk
* `compress_alg` - compress alghoritm (only for `file_dbl_save_gzip`)
  * `gzip`
  * `gzip1` - min compress
  * `gzip9` - max compress
* `file_extention` - file additional extention (only for `file_dbl_save_gzip`)


### How to run:
`app/server.app -cfg "app/stor.config.json" -log_level trace`

### Make:
`make br` - build and run clear copy (for tests).
`make tbe` - run examples one by one (1xx).
`make e_sns` - run 200_send_messages_to_queue_non_stop example. It sets new messages into queue.
`make e_g100` - run 201_get_100_messages_from_queue example. It gets first 100 messages.

