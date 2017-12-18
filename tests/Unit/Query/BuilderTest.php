<?php

namespace Vinelab\NeoEloquent\Tests\Unit\Query;

use InvalidArgumentException;
use Mockery as M;
use Vinelab\NeoEloquent\Query\Builder;
use Vinelab\NeoEloquent\Query\Grammars\CypherGrammar;
use Vinelab\NeoEloquent\Query\Processors\Processor;
use Vinelab\NeoEloquent\Tests\TestCase;

class QueryBuilderTest extends TestCase
{
    protected $grammar;

    protected $connection;

    protected $processor;

    protected $neoClient;

    protected $builder;

    public function setUp()
    {
        parent::setUp();

        $this->grammar = M::mock(\Vinelab\NeoEloquent\Query\Grammars\CypherGrammar::class)->makePartial();
        $this->connection = M::mock(\Vinelab\NeoEloquent\Connection::class)->makePartial();
        $this->neoClient = M::mock(\GraphAware\Neo4j\Client\Client::class);
        $this->processor = new Processor();

        $this->connection->shouldReceive('getClient')->andReturn($this->neoClient);
        $this->connection->shouldReceive('getQueryGrammar')->andReturn($this->grammar);

        $this->builder = new Builder($this->connection, $this->grammar, $this->processor);
    }

    public function tearDown()
    {
        M::close();

        parent::tearDown();
    }

    public function testSettingNodeLabels()
    {
        $this->builder->from(['labels']);
        $this->assertEquals(['labels'], $this->builder->from);

        $this->builder->from('User:Fan');
        $this->assertEquals('User:Fan', $this->builder->from);
    }

    public function testInsertingAndGettingId()
    {
        $label = ['Hero'];
        $this->builder->from($label);

        $values = [
            'length' => 123,
            'height' => 343,
            'power'  => 'Strong Fart Noises'
        ];

        $node = M::mock(\GraphAware\Neo4j\Client\Formatter\Type\Node::class);
        $result = M::mock(\GraphAware\Common\Result\Result::class);
        $this->neoClient->shouldReceive('run')->times(2)->andReturn($result);
        $result->shouldReceive('getRecord')->once()->andReturn($node);
        $node->shouldReceive('value')->once()->andReturn(69);

        $this->assertEquals(69, $this->builder->insertGetId($values));
    }

    public function testTransformingQueryToCypher()
    {
        $this->grammar->shouldReceive('compileSelect')->once()->with($this->builder)->andReturn(true);
        $this->assertTrue($this->builder->toCypher());
    }

    //public function testMakingLabel()
    //{
    //    $label = ['MaLabel'];
    //
    //    $this->neoClient->shouldReceive('makeLabel')->with($label)->andReturn($label);
    //    $this->assertEquals($label, $this->builder->makeLabel($label));
    //}

    /**
     * @depends testTransformingQueryToCypher
     */
    public function testSelectResult()
    {
        $cypher = 'Some cypher here';
        $this->grammar->shouldReceive('compileSelect')->once()->andReturn($cypher);
        $this->connection->shouldReceive('select')
                         ->once()
                         ->with($cypher, [])
                         ->andReturn('result');

        $result = $this->builder->getFresh();

        $this->assertEquals($result, 'result');
    }

    /**
     * @depends testTransformingQueryToCypher
     */
    public function testSelectingProperties()
    {
        $cypher = 'Some cypher here';
        $this->grammar->shouldReceive('compileSelect')->once()->andReturn($cypher);
        $this->connection->shouldReceive('select')->once()
                         ->with($cypher, [])->andReturn('result');

        $result = $this->builder->getFresh(['poop', 'head']);

        $this->assertEquals($result, 'result');
        $this->assertEquals($this->builder->columns, ['poop', 'head'], 'make sure the columns were set');
    }

    /**
     * @expectedException InvalidArgumentException
     */
    public function testFailingWhereWithNullValue()
    {
        $this->builder->where('id', '>', null);
    }

    public function testBasicWhereBindings()
    {
        $this->builder->where('id', 19);

        $this->assertEquals([
            [
                'type'     => 'Basic',
                'column'   => 'id(n)',
                'operator' => '=',
                'value'    => 19,
                'boolean'  => 'and',
                'binding'  => 'id(n)'
            ]
        ], $this->builder->wheres, 'make sure the statement was added to $wheres');
        // When the '$from' attribute is not set on the query builder, the grammar
        // will use 'n' as the default node identifier.
        $this->assertEquals(['idn' => 19], $this->builder->getBindings());
    }

    public function testBasicWhereBindingsWithFromField()
    {
        $this->builder->from = ['user'];
        $this->builder->where('id', 19);

        $this->assertEquals([
            [
                'type'     => 'Basic',
                'column'   => 'id(user)',
                'operator' => '=',
                'value'    => 19,
                'boolean'  => 'and',
                'binding'  => 'id(user)'
            ]
        ], $this->builder->wheres, 'make sure the statement was added to $wheres');
        // When no query builder is passed to the grammar then it will return 'n'
        // as node identifier by default.
        $this->assertEquals(['iduser' => 19], $this->builder->getBindings());
    }

    public function testNullWhereBindings()
    {
        $this->builder->where('farted', null);

        $this->assertEquals([
            [
                'type'    => 'Null',
                'boolean' => 'and',
                'column'  => 'farted',
                'binding' => 'farted'
            ]
        ], $this->builder->wheres);

        $this->assertEmpty($this->builder->getBindings(), 'no bindings should be added when dealing with null stuff..');
    }

    public function testWhereTransformsNodeIdBinding()
    {
        // when requesting a Node by its id we need to use
        // 'id(n)' but that won't be helpful when returned or dealt with
        // so we need to tranform it back to 'id'
        $this->builder->where('id(n)', 200);

        $this->assertEquals([
            [
                'type'     => 'Basic',
                'column'   => 'id(n)',
                'boolean'  => 'and',
                'operator' => '=',
                'value'    => 200,
                'binding'  => 'id(n)'
            ]
        ], $this->builder->wheres);

        $this->assertEquals(['idn' => 200], $this->builder->getBindings());
    }

    public function testNestedWhere()
    {
        $this->markTestIncomplete('This test has not been implemented yet.');
    }

    public function testSubWhere()
    {
        $this->markTestIncomplete('This test has not been implemented yet.');
    }

    public function testBasicSelect()
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('User');
        $this->assertEquals('MATCH (user:User) RETURN *', $builder->toCypher());
    }

    public function testBasicAlias()
    {
        $builder = $this->getBuilder();
        $builder->select('foo as bar')->from('User');

        $this->assertEquals('MATCH (user:User) RETURN user.foo as bar', $builder->toSql());
    }

    public function testAddigSelects()
    {
        $builder = $this->getBuilder();
        $builder->select('foo')->addSelect('bar')->addSelect(['baz', 'boom'])->from('User');
        $this->assertEquals('MATCH (user:User) RETURN user.foo, user.bar, user.baz, user.boom', $builder->toCypher());
    }

    public function testBasicWheres()
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('User')->where('username', '=', 'bakalazma');

        $bindings = $builder->getBindings();
        $this->assertEquals('MATCH (user:User) WHERE user.username = {username} RETURN *', $builder->toCypher());
        $this->assertEquals(['username' => 'bakalazma'], $bindings);
    }

    public function testBasicSelectDistinct()
    {
        $builder = $this->getBuilder();
        $builder->distinct()->select('foo', 'bar')->from('User');

        $this->assertEquals('MATCH (user:User) RETURN DISTINCT user.foo, user.bar', $builder->toCypher());
    }

    public function testAddBindingWithArrayMergesBindings()
    {
        $builder = $this->getBuilder();
        $builder->addBinding(['foo' => 'bar']);
        $builder->addBinding(['bar' => 'baz']);

        $this->assertEquals([
            'foo' => 'bar',
            'bar' => 'baz'
        ], $builder->getBindings());
    }

    public function testAddBindingWithArrayMergesBindingsInCorrectOrder()
    {
        $builder = $this->getBuilder();
        $builder->addBinding(['bar' => 'baz'], 'having');
        $builder->addBinding(['foo' => 'bar'], 'where');

        $this->assertEquals([
            'bar' => 'baz',
            'foo' => 'bar',
        ], $builder->getBindings());
    }

    public function testMergeBuilders()
    {
        $builder = $this->getBuilder();
        $builder->addBinding(['foo' => 'bar']);

        $otherBuilder = $this->getBuilder();
        $otherBuilder->addBinding(['baz' => 'boom']);

        $builder->mergeBindings($otherBuilder);

        $this->assertEquals([
            'foo' => 'bar',
            'baz' => 'boom',
        ], $builder->getBindings());
    }

    /*
     *  Utility functions down this line
     */

    public function setupCacheTestQuery($cache, $driver)
    {
        $connection = m::mock(\Vinelab\NeoEloquent\Connection::class);
        $connection->shouldReceive('getClient')->once()->andReturn(M::mock('Everyman\Neo4j\Client'));
        $connection->shouldReceive('getName')->andReturn('default');
        $connection->shouldReceive('getCacheManager')->once()->andReturn($cache);
        $cache->shouldReceive('driver')->once()->andReturn($driver);
        $grammar = new CypherGrammar();

        $builder = $this->getMock('Vinelab\NeoEloquent\Query\Builder', ['getFresh'], [$connection, $grammar]);
        $builder->expects($this->once())
                ->method('getFresh')
                ->with($this->equalTo(['*']))
                ->will($this->returnValue(['results']));

        return $builder->select('*')->from('User')->where('email', 'foo@bar.com');
    }

    protected function getBuilder()
    {
        $connection = M::mock(\Vinelab\NeoEloquent\Connection::class);
        $client = M::mock('Everyman\Neo4j\Client');
        $connection->shouldReceive('getClient')->once()->andReturn($client);
        $grammar = new CypherGrammar();

        return new Builder($connection, $grammar, $this->processor);
    }
}
