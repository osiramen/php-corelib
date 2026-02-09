<?php
namespace CoreLib\Db;

/**
 * Class to build sql UPDATE queries
 * @example
 *
 * (new UpdateBuilder())
 *     ->update('Race')
 *       ->values(['status' => 'open', 'start_time' => null])
 *     ->where('id', 123)
 *     ->execute()
 *
 * (new UpdateBuilder())
 *     ->update('Race')
 *       ->columns(['status = :status:', 'start_time = NULL'])
 *     ->where('id = :id:')
 *       ->bind(['status' => 'open', 'id' => 123])
 *     ->execute()
 *
 */
class UpdateBuilder extends BaseBuilder
{
	/**
	 * @var string
	 */
	protected $_tableName;

	/**
	 * @param string $model
	 * @param ?string $alias
	 * @return $this
	 * @throws Exception
	 */
	public function update($model, $alias = null)
	{
		$this->_model = null;
		$this->_tableName = $this->_protectIdentifier($model, $alias, true);
		return $this;
	}

	/**
	 * @return string
	 * @throws Exception
	 */
	protected function _buildSqlStatement(): string
	{
		if (empty($this->_tableName)) {
			throw new Exception('No table set');
		}
		if (empty($this->_columns)) {
			if (empty($this->_aValues[0])) {
				if (empty($this->_bindParams)) {
					throw new Exception('No columns set');
				} else {
					$this->_columns = array_keys($this->_bindParams);
				}
			}
		}
		$statement = 'UPDATE ';
		$statement .= $this->_tableName;
		if (!empty($this->_join)) {
			foreach ($this->_join as $join) {
				if (!empty($join['type'])) {
					$statement .= ' ';
					$statement .= $join['type'];
				}
				$statement .= ' JOIN ';
				$statement .= $join['table'];
				$statement .= ' ON (';
				$statement .= $this->_compileCondition($join['conditions']);
				$statement .= ')';
			}
		}
		$statement .= ' SET ';
		if (!empty($this->_columns)) {
			foreach ($this->_columns as $column) {
				$iPos = strpos($column, '=');
				if ($iPos > 0) {
					$value = ltrim(substr($column, $iPos + 1));
					$column = rtrim(substr($column, 0, $iPos));
				} else {
					$value = ':' . $column;
				}
				$statement .= $this->_protectIdentifier($column);
				$statement .= '=';
				$statement .= $value;
				$statement .= ', ';
			}
		} else {
			foreach ($this->_values[0] as $column => $value) {
				$statement .= $this->_protectIdentifier($column);
				$statement .= '=';
				// Serialize SqlNode instances, pass through already escaped values
				if ($value instanceof SqlNode) {
					$statement .= $this->serializeScalar($value);
				} else {
					// Already escaped by values()/set(), or raw value if $escape=false
					$statement .= $value;
				}
				$statement .= ', ';
			}
		}
		$statement = substr($statement, 0, -2);
		if (!empty($this->condition)) {
			$statement .= ' WHERE ' . $this->_compileCondition($this->condition);
		}
		if (!empty($this->_orderBy)) {
			$columns = \is_array($this->_orderBy) ? $this->_orderBy : explode(',', $this->_orderBy);
			foreach ($columns as $iIndex => $column) {
				$columns[$iIndex] = $this->_protectIdentifier($column, null, false, true);
			}
			$statement .= ' ORDER BY ';
			$statement .= implode(', ', $columns);
		}
		if (isset($this->_limit)) {
			$statement .= ' LIMIT ' . $this->_limit;
		}
		return $statement;
	}
}