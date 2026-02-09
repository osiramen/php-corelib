<?php
namespace CoreLib\Db;

/**
 * Paginator for the SelectBuilder
 */
class SelectBuilderPaginator implements PaginatorInterface
{
	/**
	 * @var SelectBuilder
	 */
	protected $_builder;

	/**
	 * @var int
	 */
	protected $_limit;

	/**
	 * @var int
	 */
	protected $_page;

	/**
	 * @var bool
	 */
	protected $_reverse;

	/**
	 * @var bool
	 */
	protected $_debug;

	/**
	 * @param array $config
	 * @throws \InvalidArgumentException
	 */
	public function __construct(array $config)
	{
		if (!isset($config['builder']) || !($config['builder'] instanceof SelectBuilder)) {
			throw new \InvalidArgumentException('Expect property "builder" an instance of "SelectBuilder"');
		}
		$this->_builder = $config['builder'];
		$this->_limit = isset($config['limit']) ? (int) $config['limit'] : 10;
		$this->_page = isset($config['page']) ? (int) $config['page'] : 1;
		$this->_reverse = isset($config['reverse']) && $config['reverse'];
		$this->_debug = isset($config['debug']) && $config['debug'];
	}

	/**
	 * Set the current page number
	 *
	 * @param int $page
	 */
	public function setCurrentPage($page)
	{
		$this->_page = $page;
	}

	public function setLimit($limit)
	{
		$this->_limit = (int) $limit;
	}

	public function getLimit()
	{
		return $this->_limit;
	}

	/**
	 * Returns a slice of the resultset to show in the pagination
	 *
	 * @param bool $bFetchObject
	 * @return \stdClass
	 */
	public function getPaginate($fetchObject = false)
	{
		$columns = $this->_builder->getColumns();
		$orderBy = $this->_builder->getOrderBy();
		$countResult = $this->_builder
			->columns(['COUNT(*) AS total_items'])
			->limit(0)
			->orderBy(null)
			->execute($this->_debug);
		$totalItems = 0;
		while ($row = $countResult->fetchArray()) {
			$totalItems += (int) $row['total_items'];
		}
		$lastPage = $this->_limit ? (int) ceil($totalItems / $this->_limit) : 1;
		$page = new \stdClass();
		$queryLimit = $this->_limit;
		$offset = $queryOffset = max(0, $this->_page - 1) * $queryLimit;
		if ($this->_page <= $lastPage) {
			if ($this->_reverse) {
				// t:60 l:50 o:0 -> 60 - 0 - 50
				// t:60 l:50 o:50 -> 60 - 50 - 50
				$queryOffset = $totalItems - $offset - $this->_limit;
				if ($queryOffset < 0) {
					$queryLimit += $queryOffset;
					$queryOffset = 0;
				}
			}
			$page->items = $this->_builder
				->columns($columns)
				->orderBy($orderBy)
				->limit($queryLimit, $queryOffset)
				->execute($this->_debug)
				->fetchAll($fetchObject ? \PDO::FETCH_OBJ : null);
			if ($this->_reverse) {
				$page->items = array_reverse($page->items);
			}
		} else {
			$page->items = [];
		}
		$page->current = max(1, $this->_page);
		$page->prev = max(1, $page->current - 1);
		$page->next = max(1, min($lastPage, $page->current + 1));
		$page->last = $lastPage;
		$page->total_pages = $lastPage;
		$page->total_items = $totalItems;
		$page->first_item = $offset + 1;
		$page->last_item = $offset + count($page->items);
		$page->limit = $this->_limit;
		return $page;
	}
}
