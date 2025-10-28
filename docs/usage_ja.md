# Tsurugi JDBCの使用方法

当文書では、Tsurugi JDBCの基本的な使用方法を説明します。

## はじめに

Tsurugi JDBCは [Tsurugi](https://github.com/project-tsurugi/tsurugidb) にアクセスするJDBCライブラリーです。

基本的な使用方法は一般的なJDBCと同様ですが、Tsurugi固有の機能も使えるようになっています。（主にトランザクションオプションの設定）

Tsurugi JDBCが準拠しているバージョンはJDBC 4.3（Java11）ですが、JDBCの全ての機能に対応しているわけではありません。対応していない機能については [Tsurugi JDBCの制限事項](limitation_ja.md) を参照してください。

## 依存ライブラリー

Tsurugi JDBCはMaven Central Repositoryで公開されています。

- https://central.sonatype.com/artifact/com.tsurugidb.jdbc/tsurugi-jdbc/overview

Gradleでの指定方法の例は [README](https://github.com/project-tsurugi/tsurugi-jdbc?tab=readme-ov-file#how-to-use) を参照してください。

Tsurugi JDBCは内部で [Tsubakuro/Java](https://github.com/project-tsurugi/tsubakuro) （Tsurugiにアクセスするライブラリー）を使用しています。  
Tsurugi JDBCを依存ライブラリーに加えると、Tsubakuro/Javaもダウンロードされます。

なお、JDBCのロガーはjava.util.loggingですが、Tsubakuro/JavaのロガーはSLF4Jです。

## JDBC URL

Tsurugi JDBCのJDBC URLの接頭辞は `jdbc:tsurugi:` です。

その直後にエンドポイントを指定します。TCP接続、IPC接続が利用できます。

- TCP接続
  - TCP/IPでTsurugiと通信します。
  - JDBC URLの例 - `jdbc:tsurugi:tcp://localhost:12345`
  - JDBC Type4（Pure Java Driver）に相当します。
- IPC接続
  - Linuxの共有メモリーを介してTsurugiと通信します。このため、Tsurugiと同一のサーバー上でしか使用できません。
  - JDBC URLの例 - `jdbc:tsurugi:ipc:tsurugi`
  - Tsubakuro/Javaがネイティブライブラリーを使用するため、JDBC Type2（Native API Driver）に相当します。
    - Tsurugi側にインストールされているネイティブライブラリーを使用するので、ユーザーがネイティブライブラリーをインストールする必要はありません。

## Tsurugiへの接続方法

以下の方法でTsurugiに接続し、Connectionを生成します。

- DriverManager
  - DriverManagerのgetConnection()で接続します。
  - [DriverManagerで接続する例](https://github.com/project-tsurugi/tsurugi-jdbc/blob/master/modules/tsurugi-jdbc-examples/src/main/java/com/tsurugidb/jdbc/example/TsurugiJdbcExample01Connect.java)
- DataSource
  - TsurugiDataSourceのgetConnection()で接続します。
  - [DataSourceで接続する例](https://github.com/project-tsurugi/tsurugi-jdbc/blob/master/modules/tsurugi-jdbc-examples/src/main/java/com/tsurugidb/jdbc/example/TsurugiJdbcExample02DataSource.java)
- ConnectionBuilder
  - TsurugiJdbcConnectionBuilderのbuild()で接続します。
  - [ConnectionBuilderで接続する例](https://github.com/project-tsurugi/tsurugi-jdbc/blob/master/modules/tsurugi-jdbc-examples/src/main/java/com/tsurugidb/jdbc/example/TsurugiJdbcExample03ConnectionBuilder.java)

## Tsurugi固有の設定

Tsurugi JDBCでは、Tsurugi固有のオプションを設定することができます。

### 設定方法

以下の方法でオプションを設定します。

- JDBC URLのクエリー文字列で設定する。
  - `jdbc:tsurugi:～?key1=value1&key2=value2` という形式
  - キー名および設定値はUTF-8でURLエンコードされている必要がある。
- DriverManagerのgetConnection()の引数Properties infoで設定する。
- DataSource（TsurugiDataSource）のセッターメソッドで設定する。
- ConnectionBuilder（TsurugiJdbcConnectionBuilder）のセッターメソッドで設定する。

また、Connectionに対して設定できる設定値もあります。

- ConnectionのsetClientInfo()で設定する。
- TsurugiJdbcConnectionにダウンキャストして、セッターメソッドで設定する。

### 主な設定値

Tsurugi JDBCで設定できる設定値には以下のようなものがあります。（抜粋）

> [!NOTE]
>
> 設定値のキー名は [TsurugiConfig](https://github.com/project-tsurugi/tsurugi-jdbc/blob/master/modules/tsurugi-jdbc/src/main/java/com/tsurugidb/jdbc/TsurugiConfig.java) で定義されています。

#### 認証

Tsurugiに接続する際の認証方法です。

- ユーザー・パスワード（ `user`, `password` ）
- 認証トークン（ `authToken` ）
- 認証ファイルのパス（ `credentials` ）

→ [認証を設定する例](https://github.com/project-tsurugi/tsurugi-jdbc/blob/master/modules/tsurugi-jdbc-examples/src/main/java/com/tsurugidb/jdbc/example/TsurugiJdbcExample11Credential.java)

#### セッションオプション

Tsurugiに接続する際のオプション（セッション情報）です。

- アプリケーション名（ `applicationName`）
- セッションラベル（ `sessionLabel` ）

#### トランザクションオプション

トランザクションを開始する際のオプションです。

- トランザクション種別（ `transactionType` ）
  - `OCC` - 実行時間が短いトランザクション（デフォルト）
  - `LTX` - 実行時間が長いトランザクション
  - `RTX` - 読み取り専用トランザクション
- トランザクションラベル（ `transactionLabel` ）
- write preserve（ `writePreserve` ）
  - テーブル名をカンマ区切りで指定する
- read area（ `inclusiveReadArea`, `exclusiveReadArea` ）
  - テーブル名をカンマ区切りで指定する
- scan parallel（ `scanParallel` ）
  - RTXの並列数

→ [トランザクションオプションを設定する例](https://github.com/project-tsurugi/tsurugi-jdbc/blob/master/modules/tsurugi-jdbc-examples/src/main/java/com/tsurugidb/jdbc/example/TsurugiJdbcExample12TransactionOption.java)

#### コミットオプション

トランザクションをコミットする際のオプションです。

- コミット種別（ `commitType` ）
  - どこまで処理したらコミットメソッドから制御が戻るか（どういう状態になるまで待つか）
    - `DEFAULT` - Tsurugiサーバー側の設定に従う（デフォルト）
    - `ACCEPTED` - コミット操作が受け付けられるまで待つ
    - `AVAILABLE` - コミットデータが他トランザクションから見えるようになるまで待つ
    - `STORED` - コミットデータがTsurugiサーバーのローカルディスクに書かれるまで待つ（永続化されるまで待つ）
    - `PROPAGATED` -コミットデータが適切な全てのノードに伝播されるまで待つ

→ [コミットオプションを設定する例](https://github.com/project-tsurugi/tsurugi-jdbc/blob/master/modules/tsurugi-jdbc-examples/src/main/java/com/tsurugidb/jdbc/example/TsurugiJdbcExample13CommitOption.java)

#### シャットダウンオプション

Connectionをクローズする際のオプションです。

- シャットダウン種別（ `shutdownType` ）
  - `NOTHING` - 実行中のリクエストの終了を待たずにクローズする
  - `GRACEFUL` - 実行中のリクエストがある場合、それらが終了するのを待ってからクローズする（デフォルト）
  - `FORCEFUL` - 実行中のリクエストがある場合、それらをキャンセルしてからクローズする

→ [シャットダウンオプションを設定する例](https://github.com/project-tsurugi/tsurugi-jdbc/blob/master/modules/tsurugi-jdbc-examples/src/main/java/com/tsurugidb/jdbc/example/TsurugiJdbcExample19ShutdownOption.java)

## コード例

```java
import java.sql.DriverManager;
import java.util.Properties;

var url = "jdbc:tsurugi:tcp://localhost:12345";
var info = new Properties();
info.setProperty("user", "tsurugi");
info.setProperty("password", "password");
info.setProperty("applicationName", "JDBC example");
info.setProperty("sessionLabel", "JDBC connection");

// 接続
try (var connection = DriverManager.getConnection(url, info)) {
    // トランザクション種別の設定
    connection.setClientInfo("transactionType", "OCC");

    try (var statement = connection.createStatement()) {
        // SQL実行（暗黙にトランザクションが開始され、デフォルトではSQLを実行する度に自動コミット）
        var sql = "update customer set c_age = 2 where c_id = 3";
        statement.executeUpdate(sql);
    }
}
```

