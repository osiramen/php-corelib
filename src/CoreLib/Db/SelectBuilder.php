<?php

namespace CoreLib\Db;

/**
 * Class to build sql SELECT queries
 * @example
 *
 * ResultSet = (new SelectBuilder())
 *     ->from('Race')
 *     ->columns(['status', 'start_time'])
 *     ->where('id', 123)
 *     ->execute()
 *
 * ResultSet = (new SelectBuilder())
 *     ->columns(['ra.status', 'ra.start_time', 'ev.category'])
 *     ->from('Race ra')
 *     ->join('Event ev', 'Event.id = Race.event_id')
 *     ->where('ra.id', 123)
 *     ->execute()
 *
 */
class SelectBuilder extends BaseBuilder
{

	/**
	 * @var array
	 */
	protected $from = [];

	/**
	 * @var array|string
	 */
	protected $groupBy;

	/**
	 * @var bool
	 */
	protected $forUpdate;

	/**
	 * @var bool
	 */
	protected $sharedLock;

	/**
	 * @var bool
	 */
	protected $distinct;

	/**
	 * @var string
	 */
	protected $injectAfterSelect;

	/**
	 * @param PdoDriver|null $db
	 * @throws Exception
	 */
	public function __construct(?PdoDriver $db = null)
	{
		parent::__construct($db, true);
	}

	/**
	 * @param bool $distinct
	 * @return $this
	 */
	public function distinct($distinct)
	{
		$this->distinct = $distinct;
		return $this;
	}

	/**
	 * @param string $inject
	 * @return $this
	 */
	public function injectAfterSelect($inject)
	{
		$this->injectAfterSelect = $inject;
		return $this;
	}

	/**
	 * Sets the models who makes part of the query
	 * @param string|array|SelectBuilder $model
	 * @param string|null $alias
	 * @return $this
	 * @throws Exception
	 */
	public function from($model, $alias = null)
	{
		$this->_model = null;
		if (is_array($model)) {
			foreach ($model as $sModel) {
				$this->from[] = $this->_protectIdentifier($sModel, null, true);
			}
		} elseif ($model instanceof SelectBuilder) {
			// sub select builder
			if (!empty($alias)) {
				$this->_tableCache[$alias] = $this->_escapeIdentifier($alias);
			}
			$this->from[] = [$model, $alias];
		} else {
			$this->from[] = $this->_protectIdentifier($model, $alias, true);
		}
		return $this;
	}

	/**
	 * Sets a GROUP BY clause
	 * @param string|array $groupBy
	 * @return $this
	 */
	public function groupBy($groupBy)
	{
		$this->groupBy = $groupBy;
		return $this;
	}

	/**
	 * Sets a FOR UPDATE clause
	 * @param bool $forUpdate
	 * @return $this
	 */
	public function forUpdate($forUpdate)
	{
		$this->forUpdate = (bool) $forUpdate;
		return $this;
	}

	/**
	 * Sets a SHARED LOCK clause
	 * @param bool $sharedLock
	 * @return $this
	 */
	public function sharedLock($sharedLock)
	{
		$this->sharedLock = (bool) $sharedLock;
		return $this;
	}

	/**
	 * @return string
	 * @throws Exception
	 */
	protected function _buildSqlStatement(): string
	{
		if (empty($this->from)) {
			throw new Exception('No models set by from()');
		}
		$statement = 'SELECT ';
		if (!empty($this->distinct)) {
			$statement .= ' DISTINCT ';
		}
		if (!empty($this->injectAfterSelect)) {
			$statement .= ' ';
			$statement .= $this->injectAfterSelect;
			$statement .= ' ';
		}
		$statement .= !empty($this->_columns) ? implode(', ', $this->_protectColumns()) : '*';
		$statement .= ' FROM ';
		foreach ($this->from as $index => $item) {
			if ($index > 0) {
				$statement .= ',';
			}
			if (is_string($item)) {
				$statement .= $item;
			}
			if (is_array($item) && $item[0] instanceof SelectBuilder) {
				$statement .= '(' . $item[0]->_buildSqlStatement() . ')';
				if (!empty($item[1])) {
					$statement .= ' AS ' . $this->_escapeIdentifier($item[1]);
				}
			}
		}
		//$statement .= implode(', ', $this->_aFrom);
		if (isset($this->_join)) {
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
		if (!empty($this->groupBy)) {
			$columns = is_array($this->groupBy) ? $this->groupBy : explode(',', $this->groupBy);
			foreach ($columns as $index => $column) {
				$columns[$index] = $this->_protectIdentifier($column, null, false, true);
			}
			$statement .= ' GROUP BY ';
			$statement .= implode(', ', $columns);
		}
		if (!empty($this->_orderBy)) {
			$columns = is_array($this->_orderBy) ? $this->_orderBy : explode(',', $this->_orderBy);
			foreach ($columns as $index => $column) {
				$columns[$index] = $this->_protectIdentifier($column, null, false, true);
			}
			$statement .= ' ORDER BY ';
			$statement .= implode(', ', $columns);
		}
		if (!empty($this->_limit)) {
			$statement .= ' LIMIT ' . $this->_limit;
			if (!empty($this->_offset)) {
				$statement .= ' OFFSET ';
				$statement .= $this->_offset;
			}
		}
		if (!empty($this->forUpdate)) {
			$statement .= ' FOR UPDATE';
		} elseif (!empty($this->sharedLock)) {
			switch ($this->db->getDriver()) {
				case 'mysql':
					$statement .= ' IN SHARED MODE';
					break;
				case 'pgsql':
					$statement .= ' FOR SHARE';
					break;
				default:
					throw new Exception("Shared lock not implemented for or by this driver");
			}
		}
		return $statement;
	}
}
