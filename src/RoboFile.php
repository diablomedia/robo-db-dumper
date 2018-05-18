<?php declare(strict_types=1);
namespace DiabloMedia\Robo;

require_once '/home/apringle/github/robo-mysqldump-php/src/Dump.php';
require_once '/home/apringle/github/robo-mysqldump-php/src/DumpData.php';
require_once '/home/apringle/github/robo-mysqldump-php/src/DumpSchema.php';
require_once '/home/apringle/github/robo-mysqldump-php/src/DumpDataPartial.php';
require_once '/home/apringle/github/robo-mysqldump-php/src/Tasks.php';
require_once '/home/apringle/github/robo-db-dumper/src/Task/Mysql/DumpGrants.php';
require_once '/home/apringle/github/robo-db-dumper/src/Task/Mysql/Tasks.php';

use DateTime;
use Exception;
use Ifsnop\Mysqldump\Mysqldump;
use Symfony\Component\Console\Input\InputOption;

abstract class RoboFile extends \Robo\Tasks
{
    use Task\MysqldumpPhp\Tasks;
    use Task\Mysql\Tasks;

    protected $hostname;
    protected $database;
    protected $password;
    protected $dsn;
    protected $username = 'root';
    protected $dumpDir;
    protected $dumpSchemaFile = '00-Schema.sql';
    protected $dumpFullFile   = '05-FullTables.sql';
    protected $dumpGrantsFile = '9999-Grants.sql';

    public function __construct()
    {
        $required = [
            'hostname', 'database', 'dumpDir'
        ];

        foreach ($required as $property) {
            if ($this->$property === null) {
                throw new Exception('Property ' . $property . ' must have a value declared');
            }
        }

        if ($this->dsn === null) {
            $this->dsn = 'mysql:host=' . $this->hostname . ';dbname=' . $this->database . ';chartset=utf8';
        }
    }

    abstract protected function getGrantUsers();
    abstract protected function getDefaultDataFilters();
    abstract protected function getDefaultDataExcludes();

    protected function getDataFilterForTable($table)
    {
        $filters = $this->getDefaultDataFilters();
        return $filters[$table] ?? false;
    }


    protected function getPassword($password = null)
    {
        if ($password) {
            $this->password = $password;
        }

        if ($this->password === null) {
            $this->password = $this->askHidden('Password for ' . $this->username . '@' . $this->hostname);
        }

        return $this->password;
    }

    public function dbDumpSchema(
        $opts = [
            'file'           => InputOption::VALUE_REQUIRED,
            'password'       => InputOption::VALUE_REQUIRED,
            'include-tables' => InputOption::VALUE_REQUIRED,
            'exclude-tables' => InputOption::VALUE_REQUIRED
        ]
    ) {
        $file = $opts['file'] ?? $this->dumpDir . DIRECTORY_SEPARATOR . $this->dumpSchemaFile;

        $includeTables = [];
        $excludeTables = [];

        $pass = $this->getPassword($opts['password']);

        if (!empty($opts['include-tables'])) {
            $includeTables = explode(',', $opts['include-tables']);
        }

        if (!empty($opts['exclude-tables'])) {
            $excludeTables = explode(',', $opts['exclude-tables']);
        }

        $this->say('Dumping prod schema (no data)');
        $this->say(sprintf('  Including: %s', empty($includeTables) ? 'All' : implode(', ', $includeTables)));
        $this->say(sprintf('  Excluding: %s', empty($excludeTables) ? 'None' : implode(', ', $excludeTables)));

        return $this->taskDumpSchema($this->dsn, $this->username, $pass)
            ->getCollectionBuilderCurrentTask()
            ->withDumpSettings([
                'include-tables' => $includeTables,
                'exclude-tables' => $excludeTables,
                'lock-tables'    => false
            ])
            ->toFile($file)
            ->run();
    }

    public function dbDumpDataFull(
        $opts = [
            'file'                    => InputOption::VALUE_REQUIRED,
            'password'                => InputOption::VALUE_REQUIRED,
            'include-tables'          => InputOption::VALUE_REQUIRED,
            'exclude-tables'          => InputOption::VALUE_REQUIRED,
            'ignore-default-excludes' => false
        ]
    ) {
        $file = $opts['file'] ?? $this->dumpDir . DIRECTORY_SEPARATOR . $this->dumpFullFile;

        $pass = $this->getPassword($opts['password']);

        $includeTables = [];
        $excludeTables = [];

        if (!empty($opts['include-tables'])) {
            $includeTables = explode(',', $opts['include-tables']);
        }

        if ($opts['ignore-default-excludes'] === false) {
            // By default, exclude all tables in the exclude & partial lists
            $excludeTables = array_merge($this->getDefaultDataExcludes(), array_keys($this->getDefaultDataFilters()));
        }

        $this->say('Dumping full tables');
        $this->say(sprintf('  Including: %s', empty($includeTables) ? 'All' : implode(', ', $includeTables)));
        $this->say(sprintf('  Excluding: %s', empty($excludeTables) ? 'None' : implode(', ', $excludeTables)));

        return $this->taskDumpData($this->dsn, $this->username, $pass)
            ->getCollectionBuilderCurrentTask()
            ->withDumpSettings([
                'include-tables' => $includeTables,
                'exclude-tables' => $excludeTables,
                'lock-tables'    => false
            ])
            ->toFile($file)
            ->run();
    }


    public function dbDumpDataPartial(
        $startDate,
        $endDate,
        $opts = [
            'dir'               => InputOption::VALUE_REQUIRED,
            'append'            => false,
            'include-tables'    => InputOption::VALUE_REQUIRED,
            'exclude-tables'    => InputOption::VALUE_REQUIRED,
            'starting-sequence' => '10',
            'password'          => InputOption::VALUE_REQUIRED,
        ]
    ) {
        $dir = $opts['dir'] ?? $this->dumpDir;

        $pass = $this->getPassword($opts['password']);

        $startDate = DateTime::createFromFormat('Y-m-d', $startDate);
        if ($startDate === false) {
            throw new Exception('Start Date provided is not in the correct format');
        }

        $endDate = DateTime::createFromFormat('Y-m-d', $endDate);
        if ($endDate === false) {
            throw new Exception('End Date provided is not in the correct format');
        }

        if (!empty($opts['include-tables'])) {
            $includeTables = explode(',', $opts['include-tables']);
        } else {
            $includeTables = array_keys($this->getDefaultDataFilters());
        }

        if (!empty($opts['exclude-tables'])) {
            $includeTables = array_diff($includeTables, explode(',', $opts['exclude-tables']));
        }

        $filters = [];
        foreach ($includeTables as $table) {
            $where = $this->getDataFilterForTable($table);
            if ($where === false) {
                throw new Exception('Table does not have partial filtering enabled: ' . $table);
            }

            $where           = str_replace(':startDate', $startDate->format("'Y-m-d'"), $where);
            $where           = str_replace(':endDate', $endDate->format("'Y-m-d'"), $where);
            $filters[$table] = $where;
        }

        $this->say('Dumping partial data for tables:');
        $this->say('  ' . implode(', ', $includeTables));

        $task = $this->taskDumpDataPartial($this->dsn, $this->username, $pass)
            ->getCollectionBuilderCurrentTask()
            ->toDir($dir)
            ->withDumpSettings(['lock-tables' => false])
            ->withFilters($filters)
            ->startingFileSequence($opts['starting-sequence']);

        if ($opts['append'] === true) {
            $task->append();
        }

        return $task->run();
    }

    public function dbDumpGrants(
        $opts = [
            'file'       => InputOption::VALUE_REQUIRED,
            'password'   => InputOption::VALUE_REQUIRED,
            'appendFile' => false
        ]
    ) {
        $file = $opts['file'] ?? $this->dumpDir . DIRECTORY_SEPARATOR . $this->dumpGrantsFile;

        $pass = $this->getPassword($opts['password']);

        $users      = $this->getGrantUsers();
        $collection = $this->collectionBuilder();

        $appendFile = $opts['appendFile'];
        foreach ($users as $user) {
            $this->say(sprintf('Dumping grants for %s@%s', $user['user'], $user['host']));
            $task = $collection->taskDumpGrants($this->dsn, $this->username, $pass)
                ->sourceUser($user['user'], $user['host'])
                ->append($appendFile)
                ->toFile($file);

            if (isset($user['pass'])) {
                $task->destinationPassword($user['pass']);
            }

            // Any additional dumps should always append to the file
            $appendFile = true;
        }

        return $collection->run();
    }

    public function dbInitializeDev(
        $startDate,
        $endDate
    ): void {
        $commands = [
            'db:dump-schema',
            'db:dump-data-full',
            'db:dump-data-partial',
            'db:dump-grants'
        ];

        $pass = $this->getPassword();

        foreach ($commands as $command) {
            if ($command == 'db:dump-data-partial') {
                $arguments = ['vendor/bin/robo', $command, $startDate, $endDate, '--password', $pass];
            } else {
                $arguments = ['vendor/bin/robo', $command, '--password', $pass];
            }
            $statusCode = \Robo\Robo::run(
                $arguments,
                self::class
            );

            if ($statusCode !== 0) {
                throw new Exception('Command ' . $command . ' failed');
            }
        }
    }
}
