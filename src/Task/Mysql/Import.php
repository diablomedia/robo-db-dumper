<?php declare(strict_types=1);

namespace DiabloMedia\Robo\Task\Mysql;

use Exception;
use Robo\Common\BuilderAwareTrait;
use Robo\Common\IO;
use Robo\Common\ResourceExistenceChecker;
use Robo\Contract\BuilderAwareInterface;
use Robo\Result;
use Robo\Task\BaseTask;

class Import extends BaseTask implements BuilderAwareInterface
{
    use BuilderAwareTrait;
    use IO;
    use ResourceExistenceChecker;

    /** @var string */
    protected $host;

    /** @var string */
    protected $db;

    /** @var string */
    protected $user;

    /** @var string */
    protected $pass;

    /** @var array */
    protected $files = [];

    /**
     * Allows us to omit the database name on CLI for specified files
     *
     * Some files may issue a CREATE DATABASE statement, and passing
     * a database name on the CLI would fail if database does not exist
     *
     * @var array
     */
    protected $filesThatDoNotRequireDbName = [];

    /** @var bool */
    protected $skipConfirmation = false;

    /** @var string */
    protected $command = 'mysql';


    public function __construct(string $host, string $db, string $user, string $pass)
    {
        $this->host = $host;
        $this->db   = $db;
        $this->user = $user;
        $this->pass = $pass;
    }

    public function withMysqlCommand($command): self
    {
        $this->command = $command;

        return $this;
    }

    /**
     * @throws Exception
     */
    public function addFile($file, bool $importRequiresDbName = true): self
    {
        if (!$this->isFile($file)) {
            throw new Exception('File does not exist: ' . $file);
        }

        $this->files[] = $file;
        if ($importRequiresDbName === false) {
            $this->filesThatDoNotRequireDbName[] = $file;
        }

        return $this;
    }

    /**
     * @throws Exception
     */
    public function addFiles(array $files, bool $importRequiresDbName = true): self
    {
        foreach ($files as $file) {
            $this->addFile($file, $importRequiresDbName);
        }

        return $this;
    }

    public function run(): Result
    {
        if (empty($this->files)) {
            return Result::error($this, 'No files are specified to import');
        }

        $collection = $this->collectionBuilder();
        foreach ($this->files as $file) {
            $collection->printTaskInfo('Importing ' . $file);
            $task = $collection->taskExec($this->command);
            $task->option('host', $this->host, '=');
            $task->option('user', $this->user, '=');
            $task->option('password', $this->pass, '=');

            if (!in_array($file, $this->filesThatDoNotRequireDbName)) {
                $task->arg($this->db);
            }

            $task->rawArg('<');
            $task->arg($file);
        }

        return $collection->run();
    }
}
