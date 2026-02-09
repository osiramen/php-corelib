<?php
/** @noinspection SqlIdentifier */
/** @noinspection SqlResolve */

namespace CoreLib\Db;

/**
 * Class to build sql INSERT queries
 * @example
 *
 * (new InsertBuilder())
 *     ->insert('Country')
 * 	   ->values(['iso2' => 'Z1', 'iso3' => 'ZZ1'])
 * 	   ->values(['iso2' => 'Z2', 'iso3' => 'ZZ2'])
 *     ->execute()
 *
 * (new InsertBuilder())
 *     ->insert('Country')
 * 	   ->values(['iso2' => 'Z1', 'iso3' => 'ZZ1', 'name' => 'Zoolooland'])
 *     ->upsert(['name' => 'Zoolooland'])
 *     ->execute()
 *
 */
class InsertBuilder extends BaseBuilder
{
	/**
	 * @var string
	 */
	protected $_table;

	/**
	 * @var bool
	 */
	protected $_replaceInto;

	/**
	 * @var bool
	 */
	protected $_upsert;

	/**
	 * @var bool
	 */
	protected $_ignore;

	/**
	 * @var array
	 */
	protected $_updateValues;

	/**
	 * @var array|string
	 */
	protected $_conflictTarget;

	protected $_returning;

	/**
	 * @param string $model
	 * @return $this
	 * @throws Exception
	 */
	public function insert($model)
	{
		$this->_model = null;
		$this->_table = $this->_protectIdentifier($model, null, true);
		$this->_replaceInto = false;
		return $this;
	}

	/**
	 * @param string $model
	 * @return $this
	 * @throws Exception
	 */
	public function replace($model)
	{
		$this->_model = null;
		$this->_table = $this->_protectIdentifier($model, null, true);
		$this->_replaceInto = true;
		return $this;
	}

	/**
	 * @param array|bool $updateValues
	 * @param bool $escape
	 * @return $this
	 */
	public function upsert($updateValues, $escape = true)
	{
		$this->_upsert = (bool) $updateValues;
		if (is_array($updateValues)) {
			if ($escape) {
				foreach ($updateValues as $column => $value) {
					// SqlNode instances are stored as-is, serialized later
					if (!($value instanceof SqlNode)) {
						$updateValues[$column] = $this->escapeValue($value);
					}
				}
			}
			$this->_updateValues = $updateValues;
		} else {
			$this->_updateValues = null;
		}
		return $this;
	}

	/**
	 * @param bool $ignore
	 * @return $this
	 */
	public function ignore($ignore)
	{
		$this->_ignore = $ignore;
		return $this;
	}

	/**
	 * @param array|string $columnsOrConstraint
	 * @return $this
	 */
	public function conflict($columnsOrConstraint)
	{
		$this->_conflictTarget = $columnsOrConstraint;
		return $this;
	}

	public function returning($columns)
	{
		if ($this->db->getDriver() !== 'pgsql') {
			throw new Exception("RETURNING is only supported by PostgreSQL");
		}
		if (!empty($columns)) {
			$this->_returning = is_array($columns) ? $columns : explode(',', $columns);
		} else {
			$this->_returning = null;
		}
		return $this;
	}

	/**
	 * @return string
	 * @throws Exception
	 */
	protected function _buildSqlStatement(): string
	{
		if (empty($this->_table)) {
			throw new Exception('No table set');
		}
		if (empty($this->_values)) {
			$fields = $this->_bindParams;
			if (empty($fields)) {
				throw new Exception('No values or bind parameters set');
			}
			if (empty($this->_columns)) {
				$this->columns($fields);
			}
		} else {
			if (empty($this->_columns)) {
				$this->columns(array_keys(reset($this->_values)));
			}
		}
		if ($this->_replaceInto) {
			switch ($this->db->getDriver()) {
				case 'mysql':
					$statement = 'REPLACE INTO ';
					break;
				case 'sqlite':
					$statement = 'INSERT OR REPLACE ';
					break;
				case 'pgsql':
					$statement = 'INSERT INTO ';
					$upsert = true;
					break;
				default:
					throw new Exception("Replace not implemented for or by this driver");
			}
		} elseif ($this->_ignore) {
			switch ($this->db->getDriver()) {
				case 'mysql':
					$statement = 'INSERT IGNORE INTO ';
					break;
				case 'sqlite':
					$statement = 'INSERT OR IGNORE INTO ';
					break;
				case 'pgsql':
					$statement = 'INSERT INTO ';
					$conflictClause = ' ON CONFLICT DO NOTHING ';
					break;
				default:
					throw new Exception("Replace not implemented for or by this driver");
			}
		} else {
			$statement = 'INSERT INTO ';
		}
		$statement .= $this->_table;
		$statement .= ' (';
		$statement .= implode(',', $this->_protectColumns());
		$statement .= ') VALUES ';
		if (isset($fields)) {
			$statement .= '(:';
			$statement .= implode(',:', $fields);
			$statement .= ')';
		} else {
			foreach ($this->_values as $values) {
				$statement .= '(';
				// Serialize values: already escaped strings pass through, SqlNode instances serialize now
				$serializedValues = [];
				foreach ($values as $value) {
					if ($value instanceof SqlNode) {
						$serializedValues[] = $this->serializeScalar($value);
					} else {
						// Already escaped by values()/set(), or raw value if $escape=false
						$serializedValues[] = $value;
					}
				}
				$statement .= implode(', ', $serializedValues);
				$statement .= '),';
			}
			$statement = substr($statement, 0, -1);
		}
		if ($this->_upsert || !empty($upsert)) {
			switch ($this->db->getDriver()) {
				case 'mysql':
					$statement .= ' ON DUPLICATE KEY UPDATE ';
					break;
				case 'sqlite':
					$statement .= ' ON CONFLICT DO UPDATE SET ';
					break;
				case 'pgsql':
					if (empty($this->_conflictTarget)) {
						throw new Exception("PostgreSQL requires a conflict target for UPSERT");
					}
					if (is_array($this->_conflictTarget)) {
						$statement .= ' ON CONFLICT (' . implode(',', $this->_protectColumns($this->_conflictTarget)) . ') DO UPDATE SET ';
					} else {
						$statement .= ' ON CONFLICT ON CONSTRAINT ' . $this->_protectIdentifier($this->_conflictTarget) . ' DO UPDATE SET ';
					}
					break;
				default:
					throw new Exception("Upsert not implemented for or by this driver");
			}
			$sep = '';
			if (!empty($this->_updateValues)) {
				foreach ($this->_updateValues as $column => $value) {
					$statement .= $sep;
					$statement .= $this->_protectIdentifier($column);
					$statement .= '=';
					// Serialize SqlNode instances in update values
					if ($value instanceof SqlNode) {
						$statement .= $this->serializeScalar($value);
					} else {
						// Already escaped by upsert() method
						$statement .= $value;
					}
					$sep = ',';
				}
			} elseif (isset($fields)) {
				foreach ($fields as $column) {
					$statement .= $sep;
					$statement .= $this->_protectIdentifier($column);
					$statement .= '=:';
					$statement .= $column;
					$sep = ',';
				}
			} else {
				if (count($this->_values) > 1) {
					throw new Exception('Upsert with multiple value sets is not supported without explicit update values');
				}
				foreach ($this->_values[0] as $column => $value) {
					$statement .= $sep;
					$statement .= $this->_protectIdentifier($column);
					$statement .= '=';
					// Serialize SqlNode instances in upsert values
					if ($value instanceof SqlNode) {
						$statement .= $this->serializeScalar($value);
					} else {
						// Already escaped by values()/set()
						$statement .= $value;
					}
					$sep = ',';
				}
			}
		} elseif (!empty($conflictClause)) {
			$statement .= $conflictClause;
		}
		if (!empty($this->_returning)) {
			$statement .= ' RETURNING ';
			$statement .= implode(', ', $this->_protectColumns($this->_returning));
			$this->_isQuery = true;
		} else {
			$this->_isQuery = false;
		}
		return $statement;
	}
}