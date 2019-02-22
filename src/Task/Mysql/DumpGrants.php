<?php declare(strict_types=1);
namespace DiabloMedia\Robo\Task\Mysql;

use PDO;
use Robo\Common\BuilderAwareTrait;
use Robo\Contract\BuilderAwareInterface;
use Robo\Result;
use Robo\Task\BaseTask;

class DumpGrants extends BaseTask implements BuilderAwareInterface
{
    use BuilderAwareTrait;

    protected $dsn;
    /**
     * @var string
     */
    protected $user;
    /**
     * @var string
     */
    protected $pass;
    /**
     * @var string
     */
    protected $file;
    /**
     * @var string
     */
    protected $sourceUser;
    /**
     * @var string
     */
    protected $sourceHost;
    /**
     * @var string
     */
    protected $sourceDatabase;
    /**
     * @var string
     */
    protected $destinationUser;
    /**
     * @var string
     */
    protected $destinationHost;
    /**
     * @var string
     */
    protected $destinationPassword;
    /**
     * @var string
     */
    protected $destinationDatabase;
    /**
     * @var bool
     */
    protected $append;

    public function __construct(string $dsn, string $user, string $pass)
    {
        $this->dsn  = $dsn;
        $this->user = $user;
        $this->pass = $pass;
    }

    public function sourceUser(string $grantUser, string $grantHost) : DumpGrants
    {
        $this->sourceUser = $grantUser;
        $this->sourceHost = $grantHost;

        return $this;
    }

    public function sourceDatabase(string $database) : DumpGrants
    {
        $this->sourceDatabase = $database;

        return $this;
    }

    public function destinationUser(string $grantUser, string $grantHost) : DumpGrants
    {
        $this->destinationUser = $grantUser;
        $this->destinationHost = $grantHost;

        return $this;
    }


    public function destinationPassword(string $password) : DumpGrants
    {
        $this->destinationPassword = $password;

        return $this;
    }
    
    public function destinationDatabase(string $destinationDatabase) : DumpGrants
    {
        $this->destinationDatabase = $destinationDatabase;

        return $this;
    }

    public function toFile(string $file) : DumpGrants
    {
        $this->file = $file;

        return $this;
    }

    public function append(bool $append = true) : DumpGrants
    {
        $this->append = $append;

        return $this;
    }

    protected function replaceUser(string $line) : string
    {
        if (!empty($this->destinationUser)) {
            $from = sprintf("/'%s'@'%s'/", $this->sourceUser, $this->sourceHost);
            $to   = sprintf("'%s'@'%s'", $this->destinationUser, $this->destinationHost);
            return preg_replace($from, $to, $line);
        }

        return $line;
    }

    protected function replacePassword(string $line) : string
    {
        // Strip IDENTIFIED BY PASSWORD <secret> section from MySQL 5.6
        $line = preg_replace('/ IDENTIFIED BY PASSWORD <secret>/', '', $line);

        if (!empty($this->destinationPassword) && preg_match('/^GRANT USAGE ON/', $line)) {
            $line .= sprintf(" IDENTIFIED BY '%s'", $this->destinationPassword);
        }

        return $line;

    }

    protected function replaceDatabase(string $line) : string
    {
        if (!empty($this->destinationDatabase)) {
            return preg_replace('/`' . $this->sourceDatabase . '`/', '`' . $this->destinationDatabase . '`', $line);
        }

        return $line;
    }

    protected function prepareLineForOutput(string $line) : string
    {
        $line = $this->replaceUser($line);
        $line = $this->replacePassword($line);
        $line = $this->replaceDatabase($line);
        return $line . ';';
    }

    public function run() : Result
    {
        if (!extension_loaded('PDO')) {
            return Result::errorMissingExtension($this, 'PDO', 'database connectivity');
        }

        if (empty($this->sourceUser) || empty($this->sourceHost)) {
            return Result::error($this, 'sourceUser must be called');
        }

        if (empty($this->file)) {
            return Result::error($this, 'Filename must be specified with toFile method');
        }

        $pdoOpt = [
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_NUM
        ];
        $pdo   = new PDO($this->dsn, $this->user, $this->pass, $pdoOpt);
        $query = "SHOW GRANTS FOR ?@?";
        $stmt  = $pdo->prepare($query);
        $stmt->execute([$this->sourceUser, $this->sourceHost]);

        if ($stmt->rowCount() === 0) {
            return Result::error($this, 'No GRANTs found for ' . $this->sourceUser . '@' . $this->sourceHost);
        }

        $collection = $this->collectionBuilder();
        $task       = $collection->taskWriteToFile($this->file)
            ->append($this->append);

        while ($row = $stmt->fetch()) {
            $task->line($this->prepareLineForOutput($row[0]));
        }

        return $collection->run();
    }
}
