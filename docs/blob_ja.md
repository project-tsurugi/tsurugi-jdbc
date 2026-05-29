# Tsurugi JDBC BLOB, CLOB使用方法

当文書では、Tsurugi JDBCでBLOB, CLOBを使用する方法を説明します。

## はじめに

Tsurugiでは、（メモリーに載せるのが躊躇われるような）大きなサイズのデータをBLOB, CLOB型で扱います。
BLOB（binary large object）はバイト列、CLOB（character large object）は文字列が対象です。

以降、BLOB, CLOBを共通で扱う場合は large object（LOB）という用語を用います。

TsurugiはインメモリーDBなので、基本的にデータは全てDBサーバーのメモリー上に置くのですが、LOBデータはメモリー上には置かず、データごとに個別のファイルとして保存されます。

クライアント（Tsurugi JDBC）とTsurugi DBの間でLOBデータを受け渡す方法はいくつかあり、どの方法を使うかはクライアントアプリケーション（Tsurugi JDBCを利用するアプリケーション）側で指定することができます。

## LOB転送モード

クライアント（Tsurugi JDBC）とTsurugi DBの間でLOBデータを受け渡す方法（LOB転送を行う方法）は、LOB転送モードと呼びます。

LOB転送モードには以下のようなものがあります。

### 特権モード

特権モードでは、クライアントアプリケーションとTsurugi DBの間でのLOBデータの受け渡しをファイル経由で行います。

そのため、LOBデータをアップロードする際は、Tsurugi JDBC内部で一時ファイルを作成し、そのファイルのパスを受け渡します。

このように特権モードではLOBデータをファイルで受け渡しするため、クライアントアプリケーションはTsurugi DBと同じサーバー上で実行する必要があります。

なお、この方式が使えるのは、Tsurugi DBのTCPエンドポイントが特権モードで稼働している場合のみです。

### BLOB中継サービス利用モード

BLOB中継サービスは、クライアントアプリケーションとTsurugi DBの間でのLOBデータの受け渡しをgRPC（TCP/IP）で行います。  
（このため、クライアントアプリケーションとTsurugi DBが異なるサーバー上で動いていても使用できます）

なお、この方式が使えるのは、BLOB中継サービスが稼働している場合のみです。

> [!NOTE]
>
> BLOB中継サービスはCLOBも扱います。（CLOB中継サービスというものはありません）

## lobTransferType

どのLOB転送モードを使うのかは、JDBC URLやTsurugiConfigのlobTransferTypeで指定します。  
TsurugiDataSourceやTsurugi JDBCのConnectionBuilderを使用する場合も同様です。

何も指定しなかった場合は `DEFAULT` として扱われます。

```java
String url = "jdbc:tsurugi:tcp://localhost:12345?lobTransferType=DEFAULT";
try (var connection = DriverManager.getConnection(url)) {
}
```

```java
import com.tsurugidb.jdbc.TsurugiJdbcLobTransferType;

var config = new TsurugiConfig();
config.setLobTransferType(TsurugiJdbcLobTransferType.DEFAULT);
try (var connection = TsurugiDriver.getTsurugiDriver().connect(config)) {
}
```

- `DEFAULT`
  - BLOB中継サービスを使用します。Tsurugi側でBLOB中継サービスが使用できなくても、コネクション生成は成功します。
- `NOT_USE`
  - LOB転送を行いません。
- `PRIVILEGED`
  - 特権モードを使用します。Tsurugi側で特権モードが使用できない場合、コネクション生成が失敗します。
- `RELAY`
  - BLOB中継サービスを使用します。Tsurugi側でBLOB中継サービスが使用できない場合、コネクション生成が失敗します。

コネクションの現在のLOB転送モードは以下の方法で取得できます。
（ここでは、`DEFAULT` が返ることはありません）

```java
TsurugiJdbcLobTransferType lobTransferType = connection.unwrap(TsurugiJdbcConnection.class).getLobTransferType()
```

## 特権モードのパスマッピング

Tsurugi JDBCでは、特権モードでLOBファイルを扱う際にクライアント側のパスとサーバー側のパスを変換するパスマッピング機能を提供しています。

特権モードでLOBファイルを扱う場合、クライアントとサーバーが同一ファイルシステムにアクセスできることを前提としていますが、環境によってはクライアントとサーバーのパスが一致しないことがあります。  
このとき、パスマッピングを設定することで、LOBファイルのパスをクライアントからサーバーに送信する際にクライアント側のパスがサーバー側のパスに変換されます。同様に、サーバーからファイルのパスを受信した際にクライアント側のパスに変換されます。

例えば以下のようにMS-Windows上のTsurugiのDockerでボリュームマウントしてTsurugi JDBCでパスマッピングを指定すると、`C:/tmp/client` に置かれたBLOBファイルをinsertすることができます。
（Tsurugi JDBCは、そこにアップロード用の一時ファイルを作成します）  
また、selectする際に、Tsurugi JDBCは `C:/tmp/tsurugi` の下にあるBLOBファイルを読みます。

```bash
docker run -d -p 12345:12345 -p 52345:52345 --name tsurugi -v C:/tmp/client:/mnt/client -v C:/tmp/tsurugi:/opt/tsurugi/var/data/log -e GLOG_v=30 ghcr.io/project-tsurugi/tsurugidb:latest
```

```java
// lobPathMappingOnSendおよびlobPathMappingOnReceiveは`<client_path>:<server_path>`形式
// ':'や'/'はURLエンコードする必要がある
String url = "jdbc:tsurugi:tcp://localhost:12345?lobTransferType=PRIVILEGED&lobPathMappingOnSend=C%3A%2Ftmp%2Fclient%3A%2Fmnt%2Fclient&lobPathMappingOnReceive=C%3A%2Ftmp%2Ftsurugi%3A%2Fopt%2Ftsurugi%2Fvar%2Fdata%2Flog";
try (var connection = DriverManager.getConnection(url)) {
}
```

```java
var config = new TsurugiConfig();
config.setLobTransferType(TsurugiJdbcLobTransferType.PRIVILEGED); // 特権モード
config.addLobPathMappingOnSend(Path.of("C:/tmp/client"), "/mnt/client");
config.addLobPathMappingOnReceive(Path.of("C:/tmp/tsurugi"), "/opt/tsurugi/var/data/log");
```

## BLOB中継サービスのエンドポイント

BLOB中継サービスの接続先URI（エンドポイント）は、Tsurugi DBから送られ、Tsurugi JDBC内部で使用しています。

しかし、ネットワーク環境によっては、Tsurugi DB内部で管理しているホスト名やIPアドレスでは、クライアントから接続できないことがあります。

このため、Tsurugi JDBCでBLOB中継サービスのエンドポイントを指定することができるようになっています。

```java
String url = "jdbc:tsurugi:tcp://localhost:12345?～&blobRelayServiceEndpoint=dns%3A%2F%2F%2Flocalhost%3A52345";
```

```java
var uri = URI.create("dns:///localhost:52345");
config.setBlobRelayServiceEndpoint(uri);
```

## BLOBの使用例

BLOBを扱う例は [examples](https://github.com/project-tsurugi/tsurugi-jdbc/blob/master/modules/tsurugi-jdbc-examples/src/main/java/com/tsurugidb/jdbc/example/TsurugiJdbcExample51Blob.java) を参照してください。