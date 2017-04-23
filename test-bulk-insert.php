<?php
/**
 * test-bulk-insert.php
 * Run a lot of INSERT statements to test the throughput of bulk data loads.
 * You need to edit the dbhost, dbuser, dbpass to suit your environment.
 *
 * Copyright 2017 Bill Karwin
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

$dbhost = '127.0.0.1';
$dbuser = 'root';
$dbpass = 'root';

$total_rows = 100*1000;
$rows_per_stmt = 1;
$stmts_per_txn = 1;
$txns_per_conn = 1;

$emulate_prepares = 0;
$indexes = 0;
$max_time = 30*60;
$mode = 'insert';
$noop = false;
$report_interval = 10.0;
$trigger = false;
$verbose = false;

function printHelp($status) {
                        echo <<<HELP
Run a bunch of insert statements

Examples:
php Test-bulk-insert.php [<options>]

--total-rows INT        Total rows to load (default: {$total_rows})
--rows-per-stmt INT     Rows per INSERT statement (default: {$row_per_stmt})
--stmts-per-txn INT     Statements per transaction (default: {$stmt_per_txn})
--txns-per-conn INT     Transactions per connection (default: {$txn_per_conn})

--emulate-prepares      PDO emulate prepared statements, or do true prepared statements
--indexes {1|2}         Create indexes for 1 or 2 attribute columns
--load-data             Use LOAD DATA INFILE instead of INSERT
--load-xml              Use LOAD XML INFILE instead of INSERT
--max-time INT          Stop running after specified number of seconds (default: {$max_time})
--noop                  Do not execute SQL statements
--report-interval FLOAT Report progress every specified number of seconds (default: {$report_interval})
--trigger               Create a trigger before INSERT
--verbose               More output

HELP;
	exit($status);
}

$shortOptions = "";
$longOptions = [
	"total-rows:",
	"rows-per-stmt:",
	"stmts-per-txn:",
	"txns-per-conn:",
	"emulate-prepares",
	"help",
	"indexes:",
	"load-data",
	"load-xml",
	"max-time:",
	"noop",
	"report-interval:",
	"trigger",
	"verbose",
];
$getopts = getopt($shortOptions, $longOptions);
if ($getopts === false) {
	echo "Failed to parse argv '" . implode(" ", $GLOBALS["argv"])
	. "' against shortOptions=$shortOptions, longOptions=" . implode(",", $longOptions)
	. ". Aborting.\n\n";
	$printHelp(1);
}
foreach ($getopts as $flag => $value) {
	switch ($flag) {
	case "total-rows":
		if ((int) $value > 0 && (int) $value < 4294967296) {
			$total_rows = (int) $value;
		}
		break;
	case "rows-per-stmt":
		if ((int) $value > 1) {
			$rows_per_stmt = (int) $value;
		}
		break;
	case "stmts-per-txn":
		if ((int) $value > 1) {
			$stmts_per_txn = (int) $value;
		}
		break;
	case "txns-per-conn":
		if ((int) $value > 1) {
			$txns_per_conn = (int) $value;
		}
		break;
	case "emulate-prepares":
		$emulate_prepares = 1;
		break;
	case "indexes":
		$indexes = (int) $value;
		break;
	case "load-data":
		$mode = "load-data";
		break;
	case "load-xml":
		$mode = "load-xml";
		break;
	case "max-time":
		if ((float) $value > 0) {
			$max_time = (float) $value;
		}
		break;
	case "noop":
		$noop = true;
		break;
	case "report-interval":
		if ((float) $value >= 1.0) {
			$report_interval = (float) $value;
		}
		break;
	case "trigger":
		$trigger = true;
		break;
	case "verbose":
		$verbose = true;
		break;
	case "help":
		printHelp(0);
	}
}

$columns = ["id", "intCol", "stringCol", "textCol"];
$columnNames = implode(",", $columns);
$parameterPlaceholders = implode(", ", array_fill(0, count($columns), "?"));

if (!$noop) {
	try {
		$pdo = new PDO("mysql:host={$dbhost};dbname=test", $dbuser, $dbpass, [
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
		]);
	} catch (PDOException $e) {
		echo "Connection failed: " . $e->getMessage() . "\n";
		exit(1);
	}
	print "Creating table\n";
	$sql = "DROP TABLE IF EXISTS TestTable";
	$pdo->query($sql);
	$sql = "
CREATE TABLE TestTable (
  id int unsigned NOT NULL PRIMARY KEY,
  intCol int unsigned DEFAULT NULL,
  stringCol varchar(100) DEFAULT NULL,
  textCol text
) ENGINE=InnoDB";
	$pdo->query($sql);
	if ($indexes >= 1) {
		print "Creating first index\n";
		$sql = "ALTER TABLE TestTable ADD INDEX (intCol)";
		$pdo->query($sql);
	}
	if ($indexes >= 2) {
		print "Creating second index\n";
		$sql = "ALTER TABLE TestTable ADD INDEX (stringCol)";
		$pdo->query($sql);
	}
	if ($trigger) {
		print "Creating a trigger before INSERT\n";
		$sql = "CREATE TRIGGER TestTrigger BEFORE INSERT ON TestTable FOR EACH ROW SET NEW.stringCol = UPPER(NEW.stringCol)";
		$pdo->query($sql);
	}
}

$conn_count = 0;
$txn_count = 0;
$stmt_count = 0;
$row_count = 0;

switch ($mode) {

case "load-data":
	$start = microtime(true);
	$datafile = "data.csv";
	$fp = fopen($datafile, "w");
	for ($row_num=0; $row_num < $total_rows; ++$row_num) {
		$data = [];
		$data[] = $row_num;
		$data[] = random_int(0, 4294967295);
		$data[] = md5($row_num);
		$data[] = str_repeat(md5($row_num), 100);
		fputcsv($fp, $data);
	}
	fclose($fp);
	$now = microtime(true);
	$duration = $now - $start;
	printf("%02d:%02d:%02d %10d rows written to CSV\n", (int) ($duration/3600), (int) ($duration/60) % 60, $duration % 60, $total_rows);
	$start = microtime(true);
	if (!$noop) {
		try {
			$pdo = new PDO("mysql:host={$dbhost};dbname=test", $dbuser, $dbpass, [
				PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
				PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
				PDO::MYSQL_ATTR_LOCAL_INFILE => true
			]);
		} catch (PDOException $e) {
			echo "Connection failed: " . $e->getMessage() . "\n";
			exit(1);
		}
		$sql = "LOAD DATA LOCAL INFILE '{$datafile}' INTO TABLE TestTable";
		$pdo->query($sql);
		$row_count += $total_rows;
		$stmt_count++;
		$txn_count++;
		$conn_count++;
	}
	$now = microtime(true);
	$duration = $now - $start;
	printf("%02d:%02d:%02d %10d rows loaded from CSV\n", (int) ($duration/3600), (int) ($duration/60) % 60, $duration % 60, $total_rows);
	break;

case "load-xml":
	$start = microtime(true);
	$datafile = "data.xml";
	$fp = fopen($datafile, "w");
	$str = '<?xml version="1.0"?>';
	fwrite($fp, $str);
	$str = '<resultset statement="SELECT * FROM test.TestTable" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">';
	fwrite($fp, $str);
	for ($row_num=0; $row_num < $total_rows; ++$row_num) {
		$data = [];
		$data["id"] = $row_count + $row_num;
		$data["intCol"] = random_int(0, 4294967295);
		$data["stringCol"] = md5($row_count . $row_num);
		$data["textCol"] = str_repeat(md5("{$row_count}{$row_num}"), 100);
		$str = '<row>';
		fwrite($fp, $str);
		foreach ($data as $field => $value) {
			$str = '<field name="';
			fwrite($fp, $str);
			fwrite($fp, $field);
			$str = '">';
			fwrite($fp, $str);
			fwrite($fp, $value);
			$str = '</field>';
			fwrite($fp, $str);
		}
		$str = '</row>';
		fwrite($fp, $str);
	}
	$str = '</resultset>';
	fwrite($fp, $str);
	fclose($fp);
	$now = microtime(true);
	$duration = $now - $start;
	printf("%02d:%02d:%02d %10d rows written to XML\n", (int) ($duration/3600), (int) ($duration/60) % 60, $duration % 60, $total_rows);
	$start = microtime(true);
	if (!$noop) {
		try {
			$pdo = new PDO("mysql:host={$dbhost};dbname=test", $dbuser, $dbpass, [
				PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
				PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
				PDO::MYSQL_ATTR_LOCAL_INFILE => true
			]);
		} catch (PDOException $e) {
			echo "Connection failed: " . $e->getMessage() . "\n";
			exit(1);
		}
		$sql = "LOAD XML LOCAL INFILE '{$datafile}' INTO TABLE TestTable";
		$pdo->query($sql);
		$row_count += $total_rows;
		$stmt_count++;
		$txn_count++;
		$conn_count++;
	}
	$now = microtime(true);
	$duration = $now - $start;
	printf("%02d:%02d:%02d %10d rows loaded from XML\n", (int) ($duration/3600), (int) ($duration/60) % 60, $duration % 60, $total_rows);
	break;

default:
	$start = microtime(true);
	$then = $start;
	while (true) {
		try {
			if (!$noop) {
				$pdo = new PDO("mysql:host={$dbhost};dbname=test", $dbuser, $dbpass, [
					PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
					PDO::ATTR_EMULATE_PREPARES => $emulate_prepares,
					PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
				]);
			}
			$conn_count++;
		} catch (PDOException $e) {
			echo "Connection failed: " . $e->getMessage() . "\n";
			exit(1);
		}
		for ($txn_num=0; $txn_num < $txns_per_conn; ++$txn_num) {
			if (!$noop) {
				$pdo->beginTransaction();
				$txn_count++;
			}
			$sql = "INSERT INTO TestTable ({$columnNames}) VALUES ";
			$tuples = [];
			for ($row_num=0; $row_num < $rows_per_stmt; ++$row_num) {
				$tuples[] = "({$parameterPlaceholders})";
			}
			$sql .= implode(", ", $tuples);
			if (!$noop) {
				$stmt = $pdo->prepare($sql);
			}
			for ($stmt_num=0; $stmt_num < $stmts_per_txn; ++$stmt_num) {
				$data = [];
				for ($row_num=0; $row_num < $rows_per_stmt; ++$row_num) {
					$data[] = $row_count + $row_num;
					$data[] = random_int(0, 4294967295);
					$data[] = md5($row_count . $row_num);
					$data[] = str_repeat(md5("{$row_count}{$row_num}"), 100);
				}
				if (!$noop) {
					$stmt->execute($data);
				}
				$stmt_count++;
				if (!$noop) {
					$row_count += $stmt->rowCount();
				} else {
					$row_count += $rows_per_stmt;
				}
				$now = microtime(true);
				if ($now - $then > $report_interval) {
					$duration = $now - $start;
					printf("%02d:%02d:%02d %10d rows %4.1f%% done\n", (int) ($duration/3600), (int) ($duration/60) % 60, $duration % 60, $row_count, $row_count * 100.0 / $total_rows);
					$then = $now;
					if ($duration > $max_time) {
						break 3;
					}
				}
				if ($row_count >= $total_rows) {
					break 3;
				}
			}
			if (!$noop) {
				$pdo->commit();
			}
		}
		$pdo = null;
		if ($row_count >= $total_rows) {
			break;
		}
	}
	break;
}

$duration = microtime(true) - $start;
printf("Time: %d seconds (%02d:%02d:%02d)\n", $duration, (int) ($duration/3600), (int) ($duration/60) % 60, $duration % 60);
printf("%10d rows = %10.2f rows/sec\n", $row_count, $row_count/$duration);
printf("%10d stmt = %10.2f stmt/sec\n", $stmt_count, $stmt_count/$duration);
printf("%10d txns = %10.2f txns/sec\n", $txn_count, $txn_count/$duration);
printf("%10d conn = %10.2f conn/sec\n", $conn_count, $conn_count/$duration);

if (!$noop) {
	printf("\n");
	try {
		$pdo = new PDO("mysql:host={$dbhost};dbname=test", $dbuser, $dbpass, [
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
			PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
		]);
	} catch (PDOException $e) {
		echo "Connection failed: " . $e->getMessage() . "\n";
		exit(1);
	}
	$global_vars = [
		'binlog_cache_size',
		'foreign_key_checks',
		'innodb_buffer_pool_size',
		'innodb_change_buffering',
		'innodb_checksum_algorithm',
		'innodb_doublewrite',
		'innodb_flush_log_at_trx_commit',
		'innodb_io_capacity',
		'innodb_log_file_size',
		'innodb_log_buffer_size',
		'innodb_lru_scan_depth',
		'log_bin',
		'sync_binlog',
	];
	$parameterPlaceholders = implode(", ", array_fill(0, count($global_vars), "?"));
	$sql = "SELECT * FROM PERFORMANCE_SCHEMA.GLOBAL_VARIABLES WHERE VARIABLE_NAME IN ($parameterPlaceholders)";
	$stmt = $pdo->prepare($sql);
	$stmt->execute($global_vars);
	foreach ($stmt->fetchAll() as $row) {
		printf("%-30s %s\n", $row["VARIABLE_NAME"], $row["VARIABLE_VALUE"]);
	}
}
