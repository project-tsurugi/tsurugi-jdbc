# Tsurugi JDBCの制限事項

当文書では、Tsurugi JDBCの主な制限事項（未対応機能）について説明します。

## 全般

Tsurugi JDBCが対応していないメソッドには `@TsurugiJdbcNotSupported` アノテーションが付けられています。
それらのメソッドを呼び出した場合、大抵はデフォルト値を返すかSQLFeatureNotSupportedExceptionがスローされます。

Tsurugi JDBCは内部で  [Tsubakuro/Java](https://github.com/project-tsurugi/tsubakuro) （Tsurugiにアクセスするライブラリー）を呼び出しているので、その制約を受けています。

## データ型

Tsurugi JDBCが対応しているデータ型は以下の通りです。

| Tsurugiの型              | java.sql.Types                   | Tsubakuro/Javaの型 | Javaの型                          | 備考          |
| ------------------------ | -------------------------------- | ------------------ | --------------------------------- | ------------- |
| （BOOLEAN）              | BIT, BOOLEAN                     | boolean            | boolean                           | Tsurugi未対応 |
| （TINYINT）              | TINYINT                          |                    | byte                              | Tsurugi未対応 |
| （SMALLINT）             | SMALLINT                         |                    | short                             | Tsurugi未対応 |
| INT                      | INTEGER                          | int                | int                               |               |
| BIGINT                   | BIGINT                           | long               | long                              |               |
| REAL                     | FLOAT, REAL                      | float              | float                             |               |
| DOUBLE                   | DOUBLE                           | double             | double                            |               |
| DECIMAL                  | NUMERIC, DECIMAL                 | BigDecimal         | BigDecimal                        |               |
| CHAR, VARCHAR            | CHAR, VARCHAR, LONGVARCHAR       | String             | String                            |               |
| BINARY, VARBINARY        | BINARY, VARBINARY, LONGVARBINARY | byte[]             | byte[]                            |               |
| BLOB                     | BLOB                             | BlobReference      | java.sql.Blob                     | 未対応        |
| CLOB                     | CLOB                             | ClobReference      | java.sql.Clob                     | 未対応        |
| DATE                     | DATE                             | LocalDate          | java.sql.Date, LocalDate          |               |
| TIME                     | TIME                             | LocalTime          | java.sql.Time, LocalTime          |               |
| TIMESTAMP                | TIMESTAMP                        | LocalDateTime      | java.sql.Timestamp, LocalDateTime |               |
| TIME WITH TIME ZONE      | TIME_WITH_TIMEZONE               | OffsetTime         | OffsetTime                        |               |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP_WITH_TIMEZONE          | OffsetDateTime     | OffsetDateTime, ZonedDateTime     |               |

- BOOLEAN, TINYINT, SMALLINTはTsurugiサーバー側が未対応です。（将来対応予定）
- BLOB, CLOBはTsurugi JDBCが未対応です。（将来対応予定）
- この表に無いjava.sql.Typesには対応していません。（XML型やROWID型等）
- ResultSet#getObject()は、基本的に「Tsubakuro/Javaの型」を返します。
  - BLOB, CLOBは未対応ですが、Tsubakuro/Javaの型でなくjava.sql.Blob, Clobを返す予定です。
- PreparedStatement#setObject()には、この表の「Javaの型」を渡すことができます。

## Connectionに関する制限事項

### トランザクションオプション

JDBCでは、SQLを実行する際に暗黙にトランザクションが開始されます。

Tsurugi JDBCでは、事前に指定されたトランザクションオプション（トランザクション種別等）を使ってトランザクションを生成します。

トランザクションオプションを設定し直した場合は、次回のトランザクション生成時から適用されます。

### setReadOnly

`setReadOnly(true)` を呼び出すと、トランザクション種別をRTX（読み取り専用トランザクション）に設定します。

`setReadOnly(false)` を呼び出すと、トランザクション種別をOCCに設定します。

### トランザクション分離レベル

Tsurugiのトランザクション分離レベルはシリアライザブル（のみ）なので、トランザクション分離レベルを変更することはできません。

### Tsurugiに無い機能

以下の機能はTsurugiに無いため、使用できません。

- プロシージャー呼び出し
- セーブポイント

## PreparedStatementに関する制限事項

PreparedStatementにセットするパラメーターの型とTsurugi内で処理されるプレースホルダーのデータ型は完全に一致している必要があります。

例えばBIGINTで処理されるプレースホルダーについては `setLong()` を使う（あるいは `setObject()` でLong値を渡す）必要があり、`setDouble()` や `setFloat()` といった異なるデータ型のメソッドを使うとTsurugi側でエラーになります。

- `setTimestamp(index, timestamp)` はTIMESTAMPとして扱います。
- `setTimestamp(index, timestamp, calendar)` はTIMESTAMP WITH TIME ZONEとして扱います。
- `setByte()`, `setShort()` はINTとして扱います。（TINYINT, SMALLINTが使えるようになったら変更する予定）

また、`getParameterMetaData()` によって返されるParameterMetaDataからは、正しい情報を取得できません。  
全てのパラメーターをセットした後であればパラメーターの個数や設定したデータ型の基本的な情報は取得できますが（桁数などの詳細な情報は取得できません）、PreparedStatementを生成した直後に取得したParameterMetaDataでパラメーターの個数を取得すると0が返ります。

> [!NOTE]
>
> Tsubakuro/JavaでPreparedStatementを生成するには、プレースホルダーが入ったSQL文の他に、プレースホルダーのデータ型も指定する必要があります。
>
> JDBCの `prepareStatement(sql)` ではプレースホルダーのデータ型が渡されないので、このメソッドが呼ばれた時点ではTsubakuro/JavaのPreparedStatementを生成することができません。そのため、初回のSQL実行までTsubakuro/JavaのPreparedStatementの生成を遅延させています。  
> したがって、`prepareStatement(sql)` から返された直後のPreparedStatementからは、メタデータを取得することができません。
>
> また、Tsubakuro/JavaのPreparedStatementを生成する際には、セットされたパラメーターの型からプレースホルダーのデータ型を推定しています。  
> ParameterMetaDataから取得される情報は セットされたパラメーターの型を元にしているため、桁数などの詳細な情報を返すことができません。

## ResultSetに関する制限事項

カラム名を使って値を取得する場合、カラム名の大文字小文字は区別されます。

ResultSetを使った更新はできません。

## DatabaseMetaDataに関する制限事項

### Tsurugiのバージョン

接続しているTsurugiのバージョンを取得することはできません。（ `getDatabaseProductVersion()` は固定値を返します）

### Tsurugiに無い機能

以下の機能（抜粋）はTsurugiが対応していないため、情報を取得できません。

- スキーマ、カタログ
- テーブル権限、カラム権限
- インデックス
- SQLキーワード
- SQL関数、プロシージャー
- ユーザー定義型（UDT）

## 仕様に関する制限事項

### SQLエスケープ構文

JDBCのSQLエスケープ構文は未対応です。

> [!NOTE]
>
> SQLエスケープ構文とは、JDBCで定義されている、DBMSに依存しない構文です。
>
> 例えば以下のような構文があります。
>
> - 日付リテラル - `{d '2025-10-21'}`
> - 時刻リテラル - `{t '09:45:30'}`
>
> 参考:  https://docs.oracle.com/cd/F25597_01/document/products/wlevs/docs30/jdbc_drivers/sqlescape.html
