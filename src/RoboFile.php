<?php declare(strict_types=1);
namespace DiabloMedia\Robo;

use DateTime;
use Exception;
use Robo\Result;
use Symfony\Component\Console\Input\InputOption;

abstract class RoboFile extends \Robo\Tasks
{
    use Task\MysqldumpPhp\Tasks;
    use Task\Mysql\Tasks;

    /**
     * @var string
     */
    protected $hostname;

    /**
     * @var string
     */
    protected $database;

    /**
     * @var string
     */
    protected $password;

    /**
     * @var string
     */
    protected $dsn;

    /**
     * @var string
     */
    protected $username = 'root';

    /**
     * @var string
     */
    protected $dumpDir;

    /**
     * @var string
     */
    protected $dumpSchemaFile = '00-Schema.sql';

    /**
     * @var string
     */
    protected $dumpFullFile   = '05-FullTables.sql';

    /**
     * @var string
     */
    protected $dumpGrantsFile = '9999-Grants.sql';

    /**
     * @var bool[]|string[]
     */
    protected $additionalDumpSettings = [];

    /**
     * RoboFile constructor.
     *
     * @throws Exception
     */
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

    /**
     * @return string[]
     */
    abstract protected function getGrantUsers();

    /**
     * @return string[]
     */
    abstract protected function getDefaultDataFilters();

    /**
     * @return string[]
     */
    abstract protected function getDefaultDataExcludes();

    /**
     * @return string|false
     */
    protected function getDataFilterForTable($table)
    {
        $filters = $this->getDefaultDataFilters();
        return $filters[$table] ?? false;
    }

    /**
     * @return string
     */
    protected function getPassword(string $password = null)
    {
        if ($password) {
            $this->password = $password;
        }

        if ($this->password === null) {
            $this->password = $this->askHidden('Password for ' . $this->username . '@' . $this->hostname);
        }

        return $this->password;
    }

    /**
     * Dump the database schema to a file
     */
    public function dbDumpSchema(
        array $opts = [
            'file'           => InputOption::VALUE_REQUIRED,
            'password'       => InputOption::VALUE_REQUIRED,
            'include-tables' => InputOption::VALUE_REQUIRED,
            'exclude-tables' => InputOption::VALUE_REQUIRED
        ]
    ) : Result {
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

        $dumpSettings = [
            'include-tables' => $includeTables,
            'exclude-tables' => $excludeTables,
            'lock-tables'    => false
        ];
        $dumpSettings = array_merge($dumpSettings, $this->additionalDumpSettings);

        return $this->taskDumpSchema($this->dsn, $this->username, $pass)
            ->getCollectionBuilderCurrentTask()
            ->withDumpSettings($dumpSettings)
            ->toFile($file)
            ->run();
    }

    /**
     * Dump data from all tables (unless excluded or defined as a partial table)
     */
    public function dbDumpDataFull(
        array $opts = [
            'file'                    => InputOption::VALUE_REQUIRED,
            'append'                  => false,
            'password'                => InputOption::VALUE_REQUIRED,
            'include-tables'          => InputOption::VALUE_REQUIRED,
            'exclude-tables'          => InputOption::VALUE_REQUIRED,
            'ignore-default-excludes' => false
        ]
    ) : Result {
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

        $dumpSettings = [
            'include-tables' => $includeTables,
            'exclude-tables' => $excludeTables,
            'lock-tables'    => false
        ];
        $dumpSettings = array_merge($dumpSettings, $this->additionalDumpSettings);

        $task = $this->taskDumpData($this->dsn, $this->username, $pass)
            ->getCollectionBuilderCurrentTask()
            ->withDumpSettings($dumpSettings)
            ->toFile($file);

        if ($opts['append'] === true) {
            $task->append();
        }

        return $task->run();
    }


    /**
     * Dump partial data from specific tables using provided filters
     *
     * @throws Exception
     */
    public function dbDumpDataPartial(
        string $startDate,
        string $endDate,
        array $opts = [
            'dir'               => InputOption::VALUE_REQUIRED,
            'append'            => false,
            'include-tables'    => InputOption::VALUE_REQUIRED,
            'exclude-tables'    => InputOption::VALUE_REQUIRED,
            'starting-sequence' => '10',
            'password'          => InputOption::VALUE_REQUIRED,
        ]
    ) : Result {
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

        $dumpSettings = [
            'lock-tables' => false
        ];
        $dumpSettings = array_merge($dumpSettings, $this->additionalDumpSettings);

        $task = $this->taskDumpDataPartial($this->dsn, $this->username, $pass)
            ->getCollectionBuilderCurrentTask()
            ->toDir($dir)
            ->withDumpSettings($dumpSettings)
            ->withFilters($filters)
            ->startingFileSequence($opts['starting-sequence']);

        if ($opts['append'] === true) {
            $task->append();
        }

        return $task->run();
    }

    /**
     * Dump CREATE GRANTS for users defined in getGrantUsers to a file
     */
    public function dbDumpGrants(
        array $opts = [
            'file'       => InputOption::VALUE_REQUIRED,
            'password'   => InputOption::VALUE_REQUIRED,
            'append'     => false
        ]
    ) : Result {
        $file = $opts['file'] ?? $this->dumpDir . DIRECTORY_SEPARATOR . $this->dumpGrantsFile;

        $pass = $this->getPassword($opts['password']);

        $users      = $this->getGrantUsers();
        $collection = $this->collectionBuilder();

        $appendFile = $opts['append'];
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

    /**
     * Initialize development database from a production source
     */
    public function dbInitializeDev(
        string $startDate,
        string $endDate
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

    /**
     * Dump data from partial and (optionally) full tables using --insert-ignore
     *
     * --exclude-full-tables will skip full table dumps (prevents db:dump-data-full from being called)
     * --overwrite-full-tables will overwrite all data in full tables (prevents --append from being passed to db:dump-data-full)
     */
    public function dbAppendDev(
        string $startDate,
        string $endDate,
        array $opts = ['exclude-full-tables' => false, 'overwrite-full-tables' => false]
    ): void {
        $commands = [
            'db:dump-data-partial',
        ];

        if ($opts['exclude-full-tables'] === true && $opts['overwrite-full-tables'] === true) {
            throw new Exception('You cannot use --exclude-full-tables and --overwrite-full-tables simultaneously!');
        }

        if ($opts['exclude-full-tables'] === false) {
            $commands[] = 'db:dump-data-full';
        }

        $pass = $this->getPassword();

        foreach ($commands as $command) {
            if ($command == 'db:dump-data-partial') {
                $arguments = ['vendor/bin/robo', $command, $startDate, $endDate, '--password', $pass, '--append'];
            } else {
                $arguments = ['vendor/bin/robo', $command, '--password', $pass];
                if ($opts['overwrite-full-tables'] === false) {
                    $arguments[] = '--append';
                }
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
