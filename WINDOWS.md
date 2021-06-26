## How to build

```
mkdir -pv app/win
GOOS="windows" go build -o ./app/win/capserver.exe ./capserver/
GOOS="windows" go build -o ./app/win/encrypt_data_generator.exe ./tools/encrypt_data_generator/
GOOS="windows" go build -o ./app/win/capsec.exe ./tools/capsec/
GOOS="windows" go build -o ./app/win/cap.exe ./tools/cap/
GOOS="windows" go build -o ./app/win/cpg.exe ./tools/cpg/
```