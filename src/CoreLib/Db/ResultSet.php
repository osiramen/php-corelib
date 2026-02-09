<?php

namespace CoreLib\Db;

use CoreLib\Mvc\Model;

/**
 * Class ResultSet
 */
class ResultSet implements \SeekableIterator, \Countable, \ArrayAccess
{
	/**
	 * @var PdoDriver
	 */
	protected $_db;

	/**
	 * @var \PDOStatement
	 */
	protected $_statement;

	/**
	 * @var null|string
	 */
	protected $_sqlStatement;

	/**
	 * @var array|null
	 */
	protected $_bindParams;

	/**
	 * @var Model|null
	 */
	protected $_model;

	/**
	 * @var int
	 */
	protected $_cursorOrientation;

	/**
	 * @var int
	 */
	protected $_cursorOffset;

	/**
	 * @var int
	 */
	protected $_fetchMode;

	/**
	 * @var bool
	 */
	protected $_autoFetchMode;

	/**
	 * @var bool
	 */
	protected $_saveState;

	/**
	 * @param PdoDriver $connection
	 * @param \PDOStatement $result
	 * @param string $sqlStatement
	 * @param array $bindParams
	 * @param array $bindTypes
	 * @param Model $model
	 * @param bool $saveState
	 */
	public function __construct($connection, \PDOStatement $result, $sqlStatement = null, $bindParams = null, $bindTypes = null, $model = null, $saveState = false)
	{
		$this->_db = $connection;
		$this->_statement = $result;
		$this->_sqlStatement = $sqlStatement;
		$this->_bindParams = $bindParams;
		$this->_model = $model;
		$this->_cursorOrientation = \PDO::FETCH_ORI_NEXT;
		$this->_cursorOffset = 0;
		$this->_fetchMode = $connection->getInternalHandler()->getAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE);
		$this->_autoFetchMode = true;
		$this->_saveState = $saveState;
	}

	/**
	 * Allows to executes the statement again. Some database systems don't support scrollable cursors,
	 * So, as cursors are forward only, we need to execute the cursor again to fetch rows from the begining
	 *
	 * @return boolean
	 */
	public function execute()
	{
		$param = $this->_bindParams;
		array_unshift($param, $this->_sqlStatement);
		$statement = call_user_func_array([$this->_db, 'query'], $param);
		if (empty($statement)) {
			return false;
		}
		$this->_statement = $statement;
		$this->_cursorOrientation = \PDO::FETCH_ORI_NEXT;
		$this->_cursorOffset = 0;
		return true;
	}

	/**
	 * Fetches an array/object of strings that corresponds to the fetched row, or FALSE if there are no more rows.
	 * This method is affected by the active fetch flag set using Phalcon\Db\Result\Pdo::setFetchMode
	 *
	 * @return mixed
	 */
	public function fetch()
	{
		$result = $this->_statement->fetch($this->_fetchMode, $this->_cursorOrientation, $this->_cursorOffset);
		$this->_cursorOrientation = \PDO::FETCH_ORI_NEXT;
		$this->_cursorOffset++;
		return $result;
	}

	/**
	 * Returns a single column from the next row of a result set or FALSE if there are no more rows.
	 * This method is affected by the active fetch flag set using Phalcon\Db\Result\Pdo::setFetchMode
	 *
	 * @param int $columnNumber
	 * @return mixed
	 */
	public function fetchColumn($columnNumber = 0)
	{
		$result = $this->_statement->fetchColumn($columnNumber);
		$this->_cursorOrientation = \PDO::FETCH_ORI_NEXT;
		$this->_cursorOffset++;
		return $result;
	}

	/**
	 * Returns an array of strings that corresponds to the fetched row, or FALSE if there are no more rows.
	 *
	 * @return mixed
	 */
	public function fetchArray()
	{
		$result = $this->_statement->fetch(\PDO::FETCH_ASSOC, $this->_cursorOrientation, $this->_cursorOffset);
		$this->_cursorOrientation = \PDO::FETCH_ORI_NEXT;
		$this->_cursorOffset++;
		return $result;
	}

	/**
	 * Returns an object that corresponds to the fetched row, or FALSE if there are no more rows.
	 *
	 * @return object
	 */
	public function fetchObject()
	{
		$result = $this->_statement->fetch(\PDO::FETCH_OBJ, $this->_cursorOrientation, $this->_cursorOffset);
		$this->_cursorOrientation = \PDO::FETCH_ORI_NEXT;
		$this->_cursorOffset++;
		return $result;
	}

	/**
	 * Returns an array of arrays containing all the records in the result
	 * This method is affected by the active fetch flag set using Phalcon\Db\Result\Pdo::setFetchMode
	 *
	 * @param int $fetchMode
	 * @return array
	 */
	public function fetchAll($fetchMode = 0)
	{
		$this->_cursorOffset = $this->_statement->rowCount();
		$this->_cursorOrientation = \PDO::FETCH_ORI_NEXT;
		return $this->_statement->fetchAll($fetchMode ?: $this->_fetchMode);
		/*
		$row = $this->_statement->fetch($this->_iFetchStyle, $this->_iCursorOrientation, $this->_iCursorOffset);
		$this->_iCursorOrientation = \PDO::FETCH_ORI_NEXT;
		$this->_iCursorOffset = 0;
		$resultArray = array();
		if ($row !== false) {
			$resultArray[] = $row;
			while ($row = $this->_statement->fetch($this->_iFetchStyle)) {
				$resultArray[] = $row;
			}
		}
		return $resultArray;
		*/
	}

	/**
	 * Fetch result in any possible kind
	 * @param array $config
	 * @return mixed
	 */
	public function fetchExt($config)
	{
		if (!empty($config['fetchMode'])) {
			$fetch_mode = $config['fetchMode'];
			if ($fetch_mode == \PDO::FETCH_CLASS && !empty($this->_model)) {
				$this->_statement->setFetchMode(\PDO::FETCH_CLASS, get_class($this->_model));
			}
		} elseif (!empty($this->_model)) {
			$this->_statement->setFetchMode(\PDO::FETCH_CLASS, get_class($this->_model));
			$fetch_mode = \PDO::FETCH_CLASS;
		} else {
			$fetch_mode = $this->_fetchMode;
		}
		if (!empty($config['keyField'])) {
			// Zeilen in Schlüsselfelder einsortieren
			$result = [];
			$key_field = $config['keyField'];
			$fetch_obj = ($fetch_mode == \PDO::FETCH_OBJ || $fetch_mode == \PDO::FETCH_CLASS);
			$fetch_value = empty($config['fetchColumn']) ? false : $config['fetchColumn'];
			$add_prefix = !empty($config['addPrefix']);
			if (is_array($key_field)) {
				while ($row = $this->_statement->fetch($fetch_mode)) {
					$ref = &$result;
					foreach ($key_field as $field_name) {
						$field_value = ($add_prefix ? $field_name . '_' : '') . ($fetch_obj ? $row->{$field_name} : $row[$field_name]);
						if (!isset($ref[$field_value])) {
							$ref[$field_value] = [];
						}
						$ref = &$ref[$field_value];
					}
					$ref = $fetch_value ? ($fetch_obj ? $row->{$fetch_value} : $row[$fetch_value]) : $row;
				}
			} else {
				while ($row = $this->_statement->fetch($fetch_mode)) {
					$field_value = ($add_prefix ? $key_field . '_' : '') . ($fetch_obj ? $row->{$key_field} : $row[$key_field]);
					$result[$field_value] = $fetch_value ? ($fetch_obj ? $row->{$fetch_value} : $row[$fetch_value]) : $row;
				}
			}
			return $result;
		} elseif (!empty($config['fetchRow'])) {
			// Nur die erste Zeile holen
			return $this->_statement->fetch($fetch_mode);
		} elseif (!empty($config['fetchColumn'])) {
			// Nur ein Feld holen
			if (is_string($config['fetchColumn'])) {
				$aRow = $this->_statement->fetch(\PDO::FETCH_ASSOC);
				return $aRow[$config['fetchColumn']];
			} else {
				$aRow = $this->_statement->fetch(\PDO::FETCH_NUM);
				return $aRow[0];
			}
		} elseif (!empty($config['fetchAll'])) {
			// Komplettes Ergebnis holen
			return $this->_statement->fetchAll($fetch_mode);
		}
		return null;
	}

	/**
	 * Gets number of rows returned by a resultset
	 *
	 * @return int
	 */
	public function numRows()
	{
		return $this->_statement->rowCount();
	}

	/**
	 * Moves internal resultset cursor to another position letting us to fetch a certain row
	 *
	 * @param int $number
	 */
	public function dataSeek($number)
	{
		$this->_iCursorOrientation = \PDO::FETCH_ORI_ABS;
		$this->_iCursorOffset = $number;
	}

	/**
	 * Changes the fetching mode affecting Phalcon\Db\Result\Pdo::fetch()
	 *
	 * @param int $fetchMode
	 * @return bool
	 */
	public function setFetchMode($fetchMode)
	{
		$this->_iFetchMode = $fetchMode;
		$this->_bAutoFetchMode = false;
		//$this->_oStatement->setFetchMode($fetchMode);
		return true;
	}

	/**
	 * Gets the internal PDO result object
	 *
	 * @return \PDOStatement
	 */
	public function getInternalResult()
	{
		return $this->_statement;
	}

	/**
	 * Returns the internal type of data retrieval that the resultset is using
	 *
	 * @return int
	 */
	public function getType()
	{
		return -1;
	}

	/**
	 * @param int $iCursorOrientation
	 * @param int $iCursorOffset
	 * @return bool|array|object|Model
	 */
	protected function _fetchRow($iCursorOrientation, $iCursorOffset = 0)
	{
		if ($this->_autoFetchMode && !empty($this->_model)) {
			$this->_statement->setFetchMode(\PDO::FETCH_CLASS, get_class($this->_model));
			$model = $this->_statement->fetch(\PDO::FETCH_CLASS, $iCursorOrientation, $iCursorOffset);
			if ($this->_saveState && method_exists($model, 'saveState')) {
				$model->saveState();
			}
			return $model;
		} else {
			return $this->_statement->fetch($this->_fetchMode, $iCursorOrientation, $iCursorOffset);
		}
	}

	/**
	 * Get first row in the resultset
	 *
	 * @return bool|array|object|Model
	 */
	public function getFirst()
	{
		$this->_iCursorOffset = 0;
		$this->_iCursorOrientation = \PDO::FETCH_ORI_NEXT;
		return $this->_fetchRow(\PDO::FETCH_ORI_FIRST);
	}

	/**
	 * Get last row in the resultset
	 *
	 * @return bool|array|object|Model
	 */
	public function getLast()
	{
		$this->_iCursorOffset = $this->_statement->rowCount();
		$this->_iCursorOrientation = \PDO::FETCH_ORI_NEXT;
		return $this->_fetchRow(\PDO::FETCH_ORI_LAST);
	}

	/**
	 * Returns a complete resultset as an array, if the resultset has a big number of rows
	 * it could consume more memory than currently it does.
	 *
	 * @return array
	 */
	public function toArray(): array
	{
		$this->_cursorOffset = $this->_statement->rowCount();
		$this->_cursorOrientation = \PDO::FETCH_ORI_NEXT;
		return $this->_statement->fetchAll(\PDO::FETCH_ASSOC);
	}

	/**
	 * Return the current element
	 * @link http://php.net/manual/en/iterator.current.php
	 * @return mixed Can return any type.
	 * @since 5.0.0
	 */
	public function current(): mixed
	{
		return $this->_fetchRow(\PDO::FETCH_ORI_ABS, $this->_cursorOffset);
	}

	/**
	 * Move forward to next element
	 * @link http://php.net/manual/en/iterator.next.php
	 * @return void Any returned value is ignored.
	 * @since 5.0.0
	 */
	public function next(): void
	{
		$this->_cursorOffset++;
	}

	/**
	 * Return the key of the current element
	 * @link http://php.net/manual/en/iterator.key.php
	 * @return mixed scalar on success, or null on failure.
	 * @since 5.0.0
	 */
	public function key(): mixed
	{
		return $this->_cursorOffset;
	}

	/**
	 * Checks if current position is valid
	 * @link http://php.net/manual/en/iterator.valid.php
	 * @return boolean The return value will be casted to boolean and then evaluated.
	 * Returns true on success or false on failure.
	 * @since 5.0.0
	 */
	public function valid(): bool
	{
		return $this->_cursorOffset < $this->_statement->rowCount();
	}

	/**
	 * Rewind the Iterator to the first element
	 * @link http://php.net/manual/en/iterator.rewind.php
	 * @return void Any returned value is ignored.
	 * @since 5.0.0
	 */
	public function rewind(): void
	{
		$this->_cursorOffset = 0;
	}

	/**
	 * Seeks to a position
	 * @link http://php.net/manual/en/seekableiterator.seek.php
	 * @param int $offset <p>
	 * The position to seek to.
	 * </p>
	 * @return void
	 * @since 5.1.0
	 */
	public function seek($offset): void
	{
		$this->_cursorOffset = $offset;
	}

	/**
	 * Count elements of an object
	 * @link http://php.net/manual/en/countable.count.php
	 * @return int The custom count as an integer.
	 * </p>
	 * <p>
	 * The return value is cast to an integer.
	 * @since 5.1.0
	 */
	public function count(): int
	{
		return $this->_statement->rowCount();
	}

	/**
	 * Whether a offset exists
	 * @link http://php.net/manual/en/arrayaccess.offsetexists.php
	 * @param mixed $offset <p>
	 * An offset to check for.
	 * </p>
	 * @return boolean true on success or false on failure.
	 * </p>
	 * <p>
	 * The return value will be casted to boolean if non-boolean was returned.
	 * @since 5.0.0
	 */
	public function offsetExists($offset): bool
	{
		return $offset >= 0 && $offset < $this->_statement->rowCount();
	}

	/**
	 * Offset to retrieve
	 * @link http://php.net/manual/en/arrayaccess.offsetget.php
	 * @param mixed $offset <p>
	 * The offset to retrieve.
	 * </p>
	 * @return mixed Can return all value types.
	 * @since 5.0.0
	 */
	public function offsetGet($offset): mixed
	{
		return $this->_fetchRow(\PDO::FETCH_ORI_ABS, $offset);
	}

	/**
	 * Offset to set
	 * @link http://php.net/manual/en/arrayaccess.offsetset.php
	 * @param mixed $offset <p>
	 * The offset to assign the value to.
	 * </p>
	 * @param mixed $value <p>
	 * The value to set.
	 * </p>
	 * @return void
	 * @since 5.0.0
	 */
	public function offsetSet($offset, $value): void
	{
		// TODO: Implement offsetSet() method.
	}

	/**
	 * Offset to unset
	 * @link http://php.net/manual/en/arrayaccess.offsetunset.php
	 * @param mixed $offset <p>
	 * The offset to unset.
	 * </p>
	 * @return void
	 * @since 5.0.0
	 */
	public function offsetUnset($offset): void
	{
		// TODO: Implement offsetUnset() method.
	}

	/**
	 * Get the query for the db listener
	 */
	public function getSQLStatement()
	{
		return $this->_sqlStatement;
	}

	/**
	 * Get bind parameters for the db listener
	 */
	public function getSQLVariables()
	{
		return $this->_bindParams;
	}

	/**
	 * Simulate Db\Adapter for the db listener
	 * @param string $str
	 * @return string
	 */
	public function escapeString($str)
	{
		return $this->_db->escapeString($str);
	}

}