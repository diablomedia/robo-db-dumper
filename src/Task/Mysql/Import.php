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

    /**
     * @var string
     */
    protected $host;
    /**
     * @var string
     */
    protected $db;
    /**
     * @var string
     */
    protected $user;
    /**
     * @var string
     */
    protected $pass;
    /**
     * @var array
     */
    protected $files = [];
    /**
     * @var bool
     */
    protected $skipConfirmation = false;
    /**
     * @var string
     */
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
    public function addFile($file): self
    {
        if (!$this->isFile($file)) {
            throw new Exception('File does not exist: ' . $file);
        }

        $this->files[] = $file;

        return $this;
    }

    /**
     * @throws Exception
     */
    public function addFiles(array $files): self
    {
        foreach ($files as $file) {
            $this->addFile($file);
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
            $collection->taskExec($this->command)
                ->option('host', $this->host, '=')
                ->option('user', $this->user, '=')
                ->option('password', $this->pass, '=')
                ->arg($this->db)
                ->rawArg('<')
                ->arg($file);
        }

        return $collection->run();
    }
}
