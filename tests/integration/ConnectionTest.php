<?php

namespace Vinelab\NeoEloquent\Tests;

use Mockery as M;

class ConnectionTest extends TestCase
{
    protected $client;

    protected $user;

    public function setUp()
    {
        parent::setUp();

        $this->user = [
            'name'     => 'Mulkave',
            'email'    => 'me@mulkave.io',
            'username' => 'mulkave',
        ];

        $this->client = $this->getClient();
    }

    public function tearDown()
    {
        M::close();

        parent::tearDown();
    }

    public function testConnection()
    {
        $c = $this->getConnectionWithConfig('neo4j');

        $this->assertInstanceOf(\Vinelab\NeoEloquent\Connection::class, $c);
    }

    public function testConnectionClientInstance()
    {
        $c = $this->getConnectionWithConfig('neo4j');

        $client = $c->getClient();

        $this->assertInstanceOf(\GraphAware\Neo4j\Client\Client::class, $client);
    }

    public function testGettingConfigParam()
    {
        $c = $this->getConnectionWithConfig('neo4j');

        $config = require(__DIR__ . '/../config/database.php');
        $this->assertEquals($c->getConfig('port'), $config['connections']['neo4j']['port']);
        $this->assertEquals($c->getConfig('host'), $config['connections']['neo4j']['host']);
    }

    public function testDriverName()
    {
        $c = $this->getConnectionWithConfig('neo4j');

        $this->assertEquals('neo4j', $c->getDriverName());
    }

    public function testGettingClient()
    {
        $c = $this->getConnectionWithConfig('neo4j');

        $this->assertInstanceOf(\GraphAware\Neo4j\Client\Client::class, $c->getClient());
    }

    public function testGettingDefaultHost()
    {
        $c = $this->getConnectionWithConfig('default');

        $this->assertEquals('localhost', $c->getHost());
        $this->assertEquals(7474, $c->getPort());
    }

    public function testGettingDefaultPort()
    {
        $c = $this->getConnectionWithConfig('default');

        $port = $c->getPort();

        $this->assertEquals(7474, $port);
        $this->assertInternalType('int', $port);
    }

    public function testGettingQueryCypherGrammar()
    {
        $c = $this->getConnectionWithConfig('default');

        $grammar = $c->getQueryGrammar();

        $this->assertInstanceOf(\Vinelab\NeoEloquent\Query\Grammars\CypherGrammar::class, $grammar);
    }

    public function testPrepareBindings()
    {
        $date = M::mock('DateTime');
        $date->shouldReceive('format')->once()->with('foo')->andReturn('bar');

        $bindings = ['test' => $date];

        $conn = $this->getMockConnection();
        $grammar = M::mock(\Vinelab\NeoEloquent\Query\Grammars\CypherGrammar::class);
        $grammar->shouldReceive('getDateFormat')->once()->andReturn('foo');
        $conn->setQueryGrammar($grammar);
        $result = $conn->prepareBindings($bindings);

        $this->assertEquals(['test' => 'bar'], $result);
    }

    public function testLogQueryFiresEventsIfSet()
    {
        $connection = $this->getMockConnection();
        $connection->setEventDispatcher($events = m::mock(\Illuminate\Events\Dispatcher::class));
        $events->shouldReceive('dispatch')->once()->with(M::type(\Illuminate\Database\Events\QueryExecuted::class));
        $connection->logQuery('foo', [], time());
    }

    public function testPretendOnlyLogsQueries()
    {
        $connection = $this->getMockConnection();
        $connection->enableQueryLog();
        $queries = $connection->pretend(function ($connection) {
            $connection->select('foo bar', ['baz']);
        });
        $this->assertEquals('foo bar', $queries[0]['query']);
        $this->assertEquals(['baz'], $queries[0]['bindings']);
    }

    public function testPreparingSimpleBindings()
    {
        $bindings = [
            'username' => 'jd',
            'name'     => 'John Doe',
        ];

        $c = $this->getConnectionWithConfig('default');

        $prepared = $c->prepareBindings($bindings);

        $this->assertEquals($bindings, $prepared);
    }

    public function testPreparingWheresBindings()
    {
        $bindings = [
            'username' => 'jd',
            'email'    => 'marie@curie.sci',
        ];

        $c = $this->getConnectionWithConfig('default');

        $prepared = $c->prepareBindings($bindings);

        $this->assertEquals($bindings, $prepared);
    }

    public function testPreparingFindByIdBindings()
    {
        $bindings = ['id' => 6];

        $c = $this->getConnectionWithConfig('default');

        $expected = ['idn' => 6];

        $prepared = $c->prepareBindings($bindings);

        $this->assertEquals($expected, $prepared);
    }

    public function testPreparingWhereInBindings()
    {
        $bindings = [
            'mc'      => 'mc',
            'ae'      => 'ae',
            'animals' => 'animals',
            'mulkave' => 'mulkave',
        ];

        $c = $this->getConnectionWithConfig('default');

        $prepared = $c->prepareBindings($bindings);

        $this->assertEquals($bindings, $prepared);
    }

    public function testGettingCypherGrammar()
    {
        $c = $this->getConnectionWithConfig('default');

        $cypher = 'MATCH (u:`User`) RETURN * LIMIT 10';
        $query = $c->getCypherQuery($cypher);

        $this->assertInternalType('array', $query);
        $this->assertArrayHasKey('statement', $query);
        $this->assertArrayHasKey('parameters', $query);
        $this->assertEquals($cypher, $query['statement']);
        $this->assertEquals([], $query['parameters']);
    }

    public function testCheckingIfBindingIsABinding()
    {
        $c = $this->getConnectionWithConfig('default');

        $empty = [];
        $valid = ['key' => 'value'];
        $invalid = [['key' => 'value']];
        $bastard = [['key' => 'value'], 'another' => 'value'];
        $bastard2 = ['another' => 'value', ['key' => 'value']];

        $this->assertFalse($c->isBinding($empty));
        $this->assertFalse($c->isBinding($invalid));
        $this->assertFalse($c->isBinding($bastard));
        $this->assertFalse($c->isBinding($bastard2));
        $this->assertTrue($c->isBinding($valid));
    }

    public function testCreatingConnection()
    {
        $c = $this->getConnectionWithConfig('default');

        $connection = $c->createConnection();

        $this->assertInstanceOf(\GraphAware\Neo4j\Client\Client::class, $connection);
    }

    public function testSelectWithBindings()
    {
        $this->createUser();

        $query = 'MATCH (n:`User`) WHERE n.username = {username} RETURN * LIMIT 1';

        $bindings = ['username' => $this->user['username']];

        $c = $this->getConnectionWithConfig('default');

        $c->enableQueryLog();
        $results = $c->select($query, $bindings);

        $log = $c->getQueryLog()[0];

        $this->assertEquals($log['query'], $query);
        $this->assertEquals($log['bindings'], $bindings);
        $this->assertInstanceOf(\GraphAware\Common\Result\Result::class, $results);

        // This is how we get the first row of the result
        // and then ask it to return its properties
        $selected = $results->getRecord()->value('n');

        $this->assertEquals($this->user, $selected->values(), 'The fetched User must be the same as the one we just created');
    }

    /**
     * @depends testSelectWithBindings
     */
    public function testSelectWithBindingsById()
    {
        // Create the User record
        $this->createUser();

        $c = $this->getConnectionWithConfig('default');
        $c->enableQueryLog();

        $query = 'MATCH (n:`User`) WHERE n.username = {username} RETURN * LIMIT 1';

        // Get the ID of the created record
        $results = $c->select($query, ['username' => $this->user['username']]);

        $node = $results->getRecord()->get('n');

        $bindings = ['id' => $node->identity()];

        // Select the Node containing the User record by its id
        $query = 'MATCH (n:`User`) WHERE id(n) = {idn} RETURN * LIMIT 1';

        $results = $c->select($query, $bindings);

        $log = $c->getQueryLog()[1];

        $this->assertEquals($log['query'], $query);
        $this->assertEquals($log['bindings'], $bindings);
        $this->assertInstanceOf(\GraphAware\Common\Result\Result::class, $results);

        $selected = $results->getRecord()->get('n');

        $this->assertEquals($this->user, $selected->values());
    }

    public function testAffectingStatement()
    {
        $c = $this->getConnectionWithConfig('default');

        $this->createUser();

        // Now we update the type and set it to 'dev'
        $query = 'MATCH (n:`User`) WHERE n.username = {username} ' .
            'SET n.type = {type}, n.updated_at = {updated_at} ' .
            'RETURN count(n)';

        $bindings = [
            'type'       => 'dev',
            'updated_at' => '2014-05-11 13:37:15',
            'username'   => $this->user['username'],
        ];

        $results = $c->affectingStatement($query, $bindings);

        $this->assertInstanceOf(\GraphAware\Common\Result\Result::class, $results);

        $this->assertEquals(1, $results->getRecord()->value('count(n)'));

        // Try to find the updated one and make sure it was updated successfully
        $query = 'MATCH (n:User) WHERE n.username = {username} RETURN n';
        $cypher = $c->getCypherQuery($query, ['username' => $this->user['username']]);

        $results = $this->client->run($cypher['statement'], $cypher['parameters']);

        $this->assertInstanceOf(\GraphAware\Common\Result\Result::class, $results);

        $user = $results->getRecord()->get('n');

        $this->assertEquals('dev', $user->value('type'));
    }

    public function testAffectingStatementOnNonExistingRecord()
    {
        $c = $this->getConnectionWithConfig('default');

        // Now we update the type and set it to 'dev'
        $query = 'MATCH (n:`User`) WHERE n.username = {username} ' .
            'SET n.type = {type}, n.updated_at = {updated_at} ' .
            'RETURN count(n)';

        $bindings = [
            ['type' => 'dev'],
            ['updated_at' => '2014-05-11 13:37:15'],
            ['username' => $this->user['username']],
        ];

        $results = $c->affectingStatement($query, $bindings);

        $this->assertInstanceOf(\GraphAware\Common\Result\Result::class, $results);

        $this->assertEquals(0, $results->getRecord()->value('count(n)'));
    }

    public function testSettingDefaultCallsGetDefaultGrammar()
    {
        $connection = $this->getMockConnection();
        $mock = M::mock('StdClass');
        $connection->expects($this->once())->method('getDefaultQueryGrammar')->will($this->returnValue($mock));
        $connection->useDefaultQueryGrammar();
        $this->assertEquals($mock, $connection->getQueryGrammar());
    }

    public function testSettingDefaultCallsGetDefaultPostProcessor()
    {
        $connection = $this->getMockConnection();
        $mock = M::mock('StdClass');
        $connection->expects($this->once())->method('getDefaultPostProcessor')->will($this->returnValue($mock));
        $connection->useDefaultPostProcessor();
        $this->assertEquals($mock, $connection->getPostProcessor());
    }

    public function testSelectOneCallsSelectAndReturnsSingleResult()
    {
        $connection = $this->getMockConnection(['select']);
        $connection->expects($this->once())
                   ->method('select')
                   ->with('foo', ['bar' => 'baz'])
                   ->will($this->returnValue(['foo']));
        $this->assertEquals('foo', $connection->selectOne('foo', ['bar' => 'baz']));
    }

    public function testInsertCallsTheStatementMethod()
    {
        $connection = $this->getMockConnection(['statement']);
        $connection->expects($this->once())->method('statement')
                   ->with($this->equalTo('foo'), $this->equalTo(['bar']))
                   ->will($this->returnValue('baz'));
        $results = $connection->insert('foo', ['bar']);
        $this->assertEquals('baz', $results);
    }

    public function testUpdateCallsTheAffectingStatementMethod()
    {
        $connection = $this->getMockConnection(['affectingStatement']);
        $connection->expects($this->once())
                   ->method('affectingStatement')
                   ->with($this->equalTo('foo'), $this->equalTo(['bar']))
                   ->will($this->returnValue('baz'));
        $results = $connection->update('foo', ['bar']);
        $this->assertEquals('baz', $results);
    }

    public function testDeleteCallsTheAffectingStatementMethod()
    {
        $connection = $this->getMockConnection(['affectingStatement']);
        $connection->expects($this->once())
                   ->method('affectingStatement')
                   ->with($this->equalTo('foo'), $this->equalTo(['bar']))
                   ->will($this->returnValue('baz'));
        $results = $connection->delete('foo', ['bar']);
        $this->assertEquals('baz', $results);
    }

    public function testBeganTransactionFiresEventsIfSet()
    {
        $connection = $this->getMockConnection(['getName']);
        $connection->expects($this->once())->method('getName')->will($this->returnValue('name'));
        $connection->setEventDispatcher($events = M::mock(\Illuminate\Events\Dispatcher::class));
        $events->shouldReceive('dispatch')
               ->once()
               ->with(M::type(\Illuminate\Database\Events\TransactionBeginning::class));
        $connection->beginTransaction();
    }

    public function testCommitedFiresEventsIfSet()
    {
        $connection = $this->getMockConnection(['getName']);
        $connection->expects($this->once())->method('getName')->will($this->returnValue('name'));
        $connection->setEventDispatcher($events = m::mock(\Illuminate\Events\Dispatcher::class));
        $events->shouldReceive('dispatch')
               ->once()
               ->with(M::type(\Illuminate\Database\Events\TransactionCommitted::class));
        $connection->commit();
    }

    public function testRollBackedFiresEventsIfSet()
    {
        $connection = $this->getMockConnection(['getName']);
        $connection->expects($this->once())->method('getName')->will($this->returnValue('name'));
        $connection->setEventDispatcher($events = M::mock(\Illuminate\Events\Dispatcher::class));
        $events->shouldReceive('dispatch')
               ->once()
               ->with(M::type(\Illuminate\Database\Events\TransactionRolledBack::class));
        $connection->rollback();
    }

    public function testTransactionMethodRunsSuccessfully()
    {
        $client = M::mock(\GraphAware\Neo4j\Client\Client::class);
        $client->shouldReceive('transaction')
               ->once()
               ->andReturn($transaction = M::mock(\GraphAware\Neo4j\Client\Transaction\Transaction::class));

        $transaction->shouldReceive('commit')
                    ->andReturn('foo');

        $connection = $this->getMockConnection();
        $connection->setClient($client);

        $result = $connection->transaction(function ($db) {
            return $db;
        });
        $this->assertEquals($connection, $result);
    }

    public function testTransactionMethodRollsbackAndThrows()
    {
        $neo = M::mock(\GraphAware\Neo4j\Client\Client::class);
        $neo->shouldReceive('transaction')
            ->once()
            ->andReturn($transaction = M::mock(\GraphAware\Neo4j\Client\Transaction\Transaction::class));

        $transaction->shouldReceive('rollback');

        $connection = $this->getMockConnection();
        $connection->setClient($neo);

        try {
            $connection->transaction(function () {
                throw new \Exception('foo');
            });
        } catch (\Exception $e) {
            $this->assertEquals('foo', $e->getMessage());
        }
    }

    public function testFromCreatesNewQueryBuilder()
    {
        $conn = $this->getMockConnection();
        $conn->setQueryGrammar(M::mock(\Vinelab\NeoEloquent\Query\Grammars\CypherGrammar::class)->makePartial());
        $conn->setPostProcessor(M::mock(\Illuminate\Database\Query\Processors\Processor::class)->makePartial());
        $builder = $conn->node('User');
        $this->assertInstanceOf(\Vinelab\NeoEloquent\Query\Builder::class, $builder);
        $this->assertEquals('User', $builder->from);
    }

    /*
     * Utility methods below this line
     */

    public function createUser()
    {
        $c = $this->getConnectionWithConfig('default');

        // First we create the record that we need to update
        $create = 'CREATE (u:User {name: {name}, email: {email}, username: {username}})';
        // The bindings structure is a little weird, I know
        // but this is how they are collected internally
        // so bare with it =)
        $createCypher = $c->getCypherQuery($create, [
            'name'     => $this->user['name'],
            'email'    => $this->user['email'],
            'username' => $this->user['username'],
        ]);

        return $this->client->sendCypherQuery($createCypher['statement'], $createCypher['parameters']);
    }

    protected function getMockConnection($methods = [])
    {
        $defaults = ['getDefaultQueryGrammar', 'getDefaultPostProcessor', 'getDefaultSchemaGrammar'];

        return $this->getMockBuilder(\Vinelab\NeoEloquent\Connection::class)
                    ->setMethods(array_merge($defaults, $methods))
                    ->setConstructorArgs([$this->dbConfig['connections']['neo4j']])
                    ->getMock();
    }
}
