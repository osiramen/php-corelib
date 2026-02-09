<?php

namespace CoreLib\Db;

/**
 * Class to build sql DELETE queries
 *
 * (new DeleteBuilder())
 *     ->from('Race')
 *     ->where('id', 123)
 *     ->execute()
 *
 */
class DeleteBuilder extends BaseBuilder
{

	/**
	 * @var array
	 */
	protected $_from = [];

	/**
	 * @var bool
	 */
	protected $_truncate;

	/**
	 * Sets the models who makes part of the query
	 * @param string|array $model
	 * @param string|null $alias
	 * @return $this
	 * @throws Exception
	 */
	public function from($model, $alias = null)
	{
		$this->_model = null;
		if (is_array($model)) {
			foreach ($model as $modelItem) {
				$this->_from[] = $this->_protectIdentifier($modelItem, null, true);
			}
		} else {
			$this->_from[] = $this->_protectIdentifier($model, $alias, true);
		}
		return $this;
	}

	/**
	 * @param bool $truncate
	 * @return $this
	 */
	public function truncate($truncate)
	{
		$this->_truncate = $truncate;
		return $this;
	}

	/**
	 * @return string
	 * @throws Exception
	 */
	protected function _buildSqlStatement(): string
	{
		if (empty($this->_from)) {
			throw new Exception('Not model set with from()');
		}
		if ($this->_truncate && empty($this->condition) && empty($this->_orderBy) && empty($this->_join)) {
			switch ($this->db->getDriver()) {
				case 'mysql':
					return 'TRUNCATE ' . implode(', ', $this->_from);
				case 'pgsql':
					return 'TRUNCATE ' . implode(', ', $this->_from) . ' RESTART IDENTITY';
			}
		}
		$statement = 'DELETE FROM ' . implode(', ', $this->_from);
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
		if (!empty($this->condition)) {
			$statement .= ' WHERE ';
			$statement .= $this->_compileCondition($this->condition);
		}
		if (!empty($this->_orderBy)) {
			$columns = is_array($this->_orderBy) ? $this->_orderBy : explode(',', $this->_orderBy);
			foreach ($columns as $index => $column) {
				$columns[$index] = $this->_protectIdentifier($column, null, false, true);
			}
			$statement .= ' ORDER BY ';
			$statement .= implode(', ', $columns);
		}
		return $statement;
	}
}