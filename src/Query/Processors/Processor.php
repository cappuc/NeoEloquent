<?php

namespace Vinelab\NeoEloquent\Query\Processors;

use Illuminate\Database\Query\Builder as BaseBuilder;
use Illuminate\Database\Query\Processors\Processor as BaseProcessor;

class Processor extends BaseProcessor
{
    /**
     * Process an  "insert get ID" query.
     *
     * @param  \Illuminate\Database\Query\Builder $query
     * @param  string $cypher
     * @param  array $values
     * @param  string $sequence
     * @return int
     */
    public function processInsertGetId(BaseBuilder $query, $cypher, $values, $sequence = null)
    {
        $query->getConnection()->insert($cypher, $values);
        $id = $query->getConnection()->lastInsertedId();

        return is_numeric($id) ? (int) $id : $id;
    }
}
