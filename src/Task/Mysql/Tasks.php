<?php declare(strict_types=1);
namespace DiabloMedia\Robo\Task\Mysql;

use Robo\Collection\CollectionBuilder;

trait Tasks
{
    protected function taskDumpGrants(string $dsn, string $user, string $pass) : CollectionBuilder
    {
        return $this->task(DumpGrants::class, $dsn, $user, $pass);
    }
}
