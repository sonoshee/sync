<?php

/* require */
require_once __DIR__.'/../vendor/autoload.php';

/* use */
use Goodby\CSV\Export\Standard\Exporter;
use Goodby\CSV\Export\Standard\ExporterConfig;
use Goodby\CSV\Export\Standard\CsvFileObject;
use Goodby\CSV\Export\Standard\Collection\PdoCollection;
use Goodby\CSV\Export\Standard\Collection\CallbackCollection;

/* Mailgun */
use Mailgun\Mailgun;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;

/* S3 */
use Aws\S3\S3Client;
use Aws\S3\Exception\S3Exception;

define("QUERY_OUTOUT_TSV_FILE", __DIR__."/result.csv");
define("QUERY_OUTPUT_CSV_FILE", __DIR__."/result_fixed.csv");
define("QUERY_INPUT_FILE", __DIR__."/query.sql");

/* load envfile */
$dotenv = new Dotenv\Dotenv(__DIR__);
$dotenv->load();

/* send mail Nishi Trainer */
$mgClient = new Mailgun($_ENV['MAILGUN_APP_KEY']);
$domain = $_ENV['DOMAIN'];


$logger = new Logger("AppLog");

$output = "[%datetime%] %level_name%: %message% %context% %extra%\n";
$formatter = new LineFormatter($output,null,true);
$stream = new StreamHandler(__DIR__."/log/app.product.log",Logger::INFO);

$logger->pushHandler($stream);
$logger->pushProcessor(function($record){
    return $record;
});


$params = [
    "prd" => [
        "user"      => $_ENV['AURORA_DB_USER'],
        "passward"  => $_ENV['AURORA_DB_PASSWORD'],
        "host"      => $_ENV['AURORA_HOST'],
        "database"  => $_ENV['AURORA_DB'],
        "exec"      => require(__DIR__."/query.php")
    ],
    "staging" => [
        "user"      => $_ENV['STAGING_AURORA_DB_USER'],
        "passward"  => $_ENV['STAGING_AURORA_DB_PASSWORD'],
        "host"      => $_ENV['STAGING_AURORA_HOST'],
        "database"  => $_ENV['STAGING_AURORA_DB'],
        "exec"      => require(__DIR__."/query.php")
    ]
];

$user           = $params[$_ENV['MODE']]['user'];
$passward       = $params[$_ENV['MODE']]['passward'];
$host           = $params[$_ENV['MODE']]['host'];
$database       = $params[$_ENV['MODE']]['database'];
$qeury_string   = $params[$_ENV['MODE']]['exec'];

try {

    $read_query_command = "`cat ".QUERY_INPUT_FILE."`";
    $exec_command = "mysql -u ${user} -p${passward} -h ${host} ${database} -e \"${read_query_command}\" -N > ".QUERY_OUTOUT_TSV_FILE;
    $exec_reg_command = "sed -e 's/\t/,/g' ".QUERY_OUTOUT_TSV_FILE." > ".QUERY_OUTPUT_CSV_FILE;

    $logger->info("prepare for exec command.",[
        "mysql_run_command"      => $exec_command,
        "shape_shell_command"    => $exec_reg_command,
    ]);

    exec($exec_command, $output, $status);

    if ($status) {
        throw new Exception("exec command failed.");
    }

    exec($exec_reg_command, $output, $status);

    if ($status) {
        throw new Exception("exec command failed.");
    }

    /* fileupload from local to s3 bucket*/
    $bucket = $_ENV['AWS_S3_BUCKET'];

    $s3 = new S3Client([
        'credentials' => [
            'key'       => $_ENV['AWS_API_KEY'],
            'secret'    => $_ENV['AWS_SECRET']
        ],
        'version'   => $_ENV['AWS_API_VERSION'],
        'region'    => $_ENV['AWS_API_REGION']
    ]);

    $results = $s3->getPaginator('ListObjects', [
        'Bucket' => $bucket,
        'Prefix' => "upload/"
    ]);

    foreach ($results as $result) {
        foreach ($result['Contents'] as $object) {
            if($object['Size'] > 0){
                $s3->deleteObject([
                    'Bucket'    => $bucket,
                    'Key'       => $object['Key']
                ]);
                $logger->info("delete s3 object ${object['Key']}.");
            }
        }
    }

    $upload_result = $s3->putObject([
        'Bucket'        => $bucket,
        "Key"           => "upload/result_fixed.csv",
        "SourceFile"    => QUERY_OUTPUT_CSV_FILE,
        "ACL"           => 'private'
    ]);

    ORM::configure($_ENV['DSN']);
    ORM::configure('username', $_ENV['DB_USER']);
    ORM::configure('password', $_ENV['PASSWARD']);

    ORM::configure('driver_options', [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_EMULATE_PREPARES => false,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);

    /* Order Copy From s3 to Redshift customers Table */
    $copy_query = file_get_contents(__DIR__.'/copy.sql');
    /* Order Delete Old Record */
    $delete_query = file_get_contents(__DIR__.'/delete.sql');

    $logger->info("prepare for redshift operation query.",[
        "copy_query"      => $copy_query,
        "delete_query"    => $delete_query,
    ]);

    $pdo = ORM::get_db();

    $stmt = $pdo->query($delete_query);
    $result = $stmt->fetch(PDO::FETCH_ASSOC);
    $stmt = NULL;

    $stmt = $pdo->query($copy_query);
    $stmt = NULL;

    $logger->info("end jobs normally.");
    $title = "[通知]最新データのCopy完了";
    $body = $altbody = "正常にcustomersテーブルのResyncが完了しました。";


}catch(S3Exception $e){

    $context = [
        "file"      => $e->getFile(),
        "line"      => $e->getLine(),
        "trace"     => $e->getTraceAsString()
    ];

    $logger->error(
        $e->getMessage(),
        $context
    );

    $title = "[S3]Exception";
    $body = $altbody = $e->getMessage();

} catch (PDOException $e) {

    $context = [
        "file"      => $e->getFile(),
        "line"      => $e->getLine(),
        "trace"     => $e->getTraceAsString()
    ];

    $logger->error(
        $e->getMessage(),
        $context
    );

    $title = "[PDO]Exception";
    $body = $altbody = $e->getMessage();

}catch(Exception $e){

    $context = [
        "file"      => $e->getFile(),
        "line"      => $e->getLine(),
        "trace"     => $e->getTraceAsString()
    ];

    $logger->error(
        $e->getMessage(),
        $context
    );

    $title = "[Global]Exception";
    $body = $altbody = $e->getMessage();

}finally{

    $result = $mgClient->sendMessage($domain, [
        'from'    => 'RRK-Dev <dev@mail.ikiikitown.net>',
        'to'      => 'k-tsujito@riracle.com',
        'subject' => $title,
        'html'    => $altbody,
        'text'    => $body
        ]
    );

}