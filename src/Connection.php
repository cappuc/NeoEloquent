<?php

namespace Vinelab\NeoEloquent;

use Closure;
use DateTimeInterface;
use Exception;
use GraphAware\Common\Result\Result;
use GraphAware\Neo4j\Client\Client;
use GraphAware\Neo4j\Client\ClientBuilder;
use Illuminate\Database\Connection as BaseConnection;
use Vinelab\NeoEloquent\Query\Builder as QueryBuilder;
use Vinelab\NeoEloquent\Query\Grammars\CypherGrammar as QueryGrammar;
use Vinelab\NeoEloquent\Query\Processors\Processor;

class Connection extends BaseConnection
{
    /**
     * The Neo4j active client connection
     *
     * @var \GraphAware\Neo4j\Client\Client
     */
    protected $neo;

    /**
     * The Neo4j database transaction
     *
     * @var \GraphAware\Neo4j\Client\Transaction\Transaction
     */
    protected $transaction;

    /**
     * Default connection configuration parameters
     *
     * @var array
     */
    protected $defaults = [
        'host'     => 'localhost',
        'port'     => 7474,
        'username' => null,
        'password' => null,
        'ssl'      => false
    ];

    /**
     * The neo4j driver name
     *
     * @var string
     */
    protected $driverName = 'neo4j';

    /**
     * The query post processor implementation.
     *
     * @var \Illuminate\Database\Query\Processors\Processor
     */
    protected $postProcessor;

    /**
     * Create a new database connection instance
     *
     * @param array $config The database connection configuration
     */
    public function __construct(array $config = [])
    {
        $this->config = $config;

        // activate and set the database client connection
        $this->neo = $this->createConnection();

        // We need to initialize a query grammar and the query post processors
        // which are both very important parts of the database abstractions
        // so we initialize these to their default values while starting.
        $this->useDefaultQueryGrammar();
        $this->useDefaultPostProcessor();
    }

    /**
     * Create a new Neo4j client
     *
     * @return \GraphAware\Neo4j\Client\Client
     */
    public function createConnection()
    {
        return ClientBuilder::create()
                            ->addConnection('default', $this->getHttpUrl())
                            ->build();
    }

    /**
     * Build Http Url for connection
     *
     * @return string
     */
    protected function getHttpUrl()
    {
        $method = $this->getSsl() ? 'https' : 'http';

        return "{$method}://{$this->getUsername()}:{$this->getPassword()}@{$this->getHost()}:{$this->getPort()}";
    }

    /**
     * Get the currently active database client
     *
     * @return \GraphAware\Neo4j\Client\Client
     */
    public function getClient()
    {
        return $this->neo;
    }

    /**
     * Set the client responsible for the
     * database communication
     *
     * @param \GraphAware\Neo4j\Client\Client $client
     */
    public function setClient(Client $client)
    {
        $this->neo = $client;
    }

    /**
     * Get the connection host
     *
     * @return string
     */
    public function getHost()
    {
        return $this->getConfig('host');
    }

    /**
     * Get the connection port
     *
     * @return int|string
     */
    public function getPort()
    {
        return $this->getConfig('port');
    }

    /**
     * Get the connection username
     *
     * @return int|string
     */
    public function getUsername()
    {
        return $this->getConfig('username');
    }

    /**
     * Get the connection password
     *
     * @return int|string
     */
    public function getPassword()
    {
        return $this->getConfig('password');
    }

    /**
     * Get the connection ssl setting
     *
     * @return bool
     */
    public function getSsl()
    {
        return $this->getConfig('ssl');
    }

    /**
     * Get the Neo4j driver name.
     *
     * @return string
     */
    public function getDriverName()
    {
        return $this->driverName;
    }

    /**
     * Get the default query grammar instance.
     *
     * @return \Vinelab\NeoEloquent\Query\Grammars\CypherGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        return new QueryGrammar();
    }

    /**
     * Get the default post processor instance.
     *
     * @return \Vinelab\NeoEloquent\Query\Processors\Processor
     */
    protected function getDefaultPostProcessor()
    {
        return new Processor();
    }

    /**
     * Run a select statement against the database.
     *
     * @param  string $query
     * @param  array $bindings
     * @param  bool $useReadPdo
     * @return array
     * @throws \Vinelab\NeoEloquent\QueryException
     */
    public function select($query, $bindings = [], $useReadPdo = false)
    {
        return $this->run($query, $bindings, function (self $me, $query, array $bindings) {
            if ($me->pretending()) {
                return [];
            }

            // For select statements, we'll simply execute the query and return an array
            // of the database result set. Each element in the array will be a single
            // node from the database, and will either be an array or objects.
            $query = $me->getCypherQuery($query, $bindings);

            return $this->getClient()
                        ->run($query['statement'], $query['parameters']);
        });
    }
    //
    ///**
    // * Run an insert statement against the database.
    // *
    // * @param string $query
    // * @param array $bindings
    // *
    // * @return mixed
    // * @throws \Vinelab\NeoEloquent\QueryException
    // */
    //public function insert($query, $bindings = array())
    //{
    //    return $this->statement($query, $bindings, true);
    //}
    //
    /**
     * Run a Cypher statement and get the number of nodes affected.
     *
     * @param  string $query
     * @param  array $bindings
     * @return int
     * @throws \Vinelab\NeoEloquent\QueryException
     */
    public function affectingStatement($query, $bindings = [])
    {
        $result = $this->statement($query, $bindings, true);

        $this->recordsHaveBeenModified($result->summarize()->updateStatistics()->containsUpdates());

        return $result->hasRecord() ? $result->getRecord()->values()[0] : 0;
    }

    /**
     * Execute a Cypher statement and return the boolean result.
     *
     * @param  string $query
     * @param  array $bindings
     * @param bool $rawResults
     * @return bool|\GraphAware\Common\Result\Result When $rawResult is set to true.
     * @throws \Vinelab\NeoEloquent\QueryException
     */
    public function statement($query, $bindings = [], $rawResults = false)
    {
        return $this->run($query, $bindings, function (self $me, $query, array $bindings) use ($rawResults) {
            if ($me->pretending()) {
                return true;
            }

            $query = $me->getCypherQuery($query, $bindings);

            $result = $this->getClient()
                           ->run($query['statement'], $query['parameters']);

            return ($rawResults === true) ? $result : $result instanceof Result;
        });
    }

    /**
     * Make a query out of a Cypher statement
     * and the bindings values.
     *
     * @param string $query
     * @param array $bindings
     * @return array
     */
    public function getCypherQuery($query, array $bindings = [])
    {
        return ['statement' => $query, 'parameters' => $this->prepareBindings($bindings)];
    }

    /**
     * Prepare the query bindings for execution.
     *
     * @param  array $bindings
     * @return array
     */
    public function prepareBindings(array $bindings)
    {
        $grammar = $this->getQueryGrammar();

        $prepared = [];

        foreach ($bindings as $key => $value) {
            // We need to transform all instances of DateTimeInterface into the actual
            // date string. Each query grammar maintains its own date string format
            // so we'll just ask the grammar for the format to get from the date.

            $property = is_numeric($key) || $key == 'id' ? $grammar->getIdReplacement($key) : $key;

            if ($value instanceof DateTimeInterface) {
                $prepared[$property] = $value->format($grammar->getDateFormat());
            } else if (is_bool($value)) {
                $prepared[$property] = (int) $value;
            } else {
                $prepared[$property] = $value;
            }
        }

        return $prepared;
    }

    /**
     * A binding should always be in an associative
     * form of a key=>value, otherwise we will not be able to
     * consider it a valid binding and replace its values in the query.
     * This function validates whether the binding is valid to be used.
     *
     * @param  array $binding
     * @return boolean
     */
    public function isBinding(array $binding)
    {
        if (! empty($binding)) {
            // A binding is valid only when the key is not a number
            return collect($binding)->keys()->reduce(function ($final, $key) {
                return $final && ! is_numeric($key);
            }, true);
        }

        return false;
    }

    /**
     * Start a new database transaction.
     *
     * @return void
     */
    public function beginTransaction()
    {
        ++$this->transactions;

        if ($this->transactions == 1) {
            $this->transaction = $this->neo->transaction();
        }

        $this->fireConnectionEvent('beganTransaction');
    }

    /**
     * Commit the active database transaction.
     *
     * @return void
     */
    public function commit()
    {
        if ($this->transactions == 1) {
            $this->transaction->commit();
        }

        --$this->transactions;

        $this->fireConnectionEvent('committed');
    }

    /**
     * Rollback the active database transaction.
     *
     * @param null $toLevel
     * @return void
     */
    public function rollBack($toLevel = null)
    {
        if ($this->transactions == 1) {
            $this->transactions = 0;

            $this->transaction->rollBack();
        } else {
            --$this->transactions;
        }

        $this->fireConnectionEvent('rollingBack');
    }

    /**
     * Begin a fluent query against a node.
     *
     * @param string $label
     *
     * @return \Vinelab\NeoEloquent\Query\Builder
     */
    public function node($label)
    {
        $query = new QueryBuilder($this, $this->getQueryGrammar(), $this->getPostProcessor());

        return $query->from($label);
    }

    /**
     * Begin a fluent query against a database table.
     * In neo4j's terminologies this is a node.
     *
     * @param  string $table
     * @return \Vinelab\NeoEloquent\Query\Builder
     */
    public function table($table)
    {
        return $this->node($table);
    }

    /**
     * Run a Cypher statement and log its execution context.
     *
     * @param  string $query
     * @param  array $bindings
     * @param  Closure $callback
     * @return mixed
     *
     * @throws QueryException
     */
    protected function run($query, $bindings, Closure $callback)
    {
        $start = microtime(true);

        // To execute the statement, we'll simply call the callback, which will actually
        // run the Cypher against the Neo4j connection. Then we can calculate the time it
        // took to execute and log the query Cypher, bindings and time in our memory.
        try {
            $result = $callback($this, $query, $bindings);
        }
            // If an exception occurs when attempting to run a query, we'll format the error
            // message to include the bindings with Cypher, which will make this exception a
            // lot more helpful to the developer instead of just the database's errors.
        catch (Exception $e) {
            throw new QueryException($query, $bindings, $e);
        }

        // Once we have run the query we will calculate the time that it took to run and
        // then log the query, bindings, and execution time so we will report them on
        // the event that the developer needs them. We'll log time in milliseconds.
        $time = $this->getElapsedTime($start);

        $this->logQuery($query, $bindings, $time);

        return $result;
    }
    ///**
    // * Get the default schema grammar instance.
    // *
    // * @return \Illuminate\Database\Schema\Grammars\Grammar
    // */
    //protected function getDefaultSchemaGrammar()
    //{
    //    return new SchemaGrammar();
    //}

    /**
     * Get the last Id created by Neo4J
     *
     * @return int
     * @throws \Vinelab\NeoEloquent\QueryException
     */
    public function lastInsertedId()
    {
        $query = "MATCH (n) RETURN MAX(id(n)) AS lastIdCreated";

        $result = $this->statement($query, [], true)->getRecord();

        return $result->value('lastIdCreated');
    }
}
