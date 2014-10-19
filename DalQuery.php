<?php
/**
 * Description of DAL_query
 *
    Copyright 2011-2014 hatwebtech.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 *
 * @license Apache License, Version 2.0  http://www.apache.org/licenses/LICENSE-2.0
 *
 * @author Panajotis Zamos
 *
 * @todo mearge BIND and BIND_WHERE params (order of ? in sql and order of binding not consistent)
 * @todo other JOIN types
 * @todo implement GROUP BY and HAVING
 * @todo implement execute() to somehow avoid checking for error after queryStmt()
 *
 * @todo BUG where condition 'alias.field IS NULL' don't work if there is no space at the end of string (after 'NULL')
 * @todo BUG join condition not applied if joining Table object have behaviors
 * @todo BUG 'alias.bool_field = ?' => true DON'T work but 'alias.bool_field = ?' => 1 do work?
 */
namespace hatwebtech\dal;
class DalQuery {
  protected $_dbh;
  protected $_table_path;
  protected $_table_namespace = '\\';

  protected $_where_operands = array('AND', 'OR');
  protected $_where_predicates = array(
        'simple' => array('<>', '>=', '<=', '>', '<', '=', ),
        'complex' => array('NOT IN', 'IN', 'NOT BETWEEN', 'BETWEEN', 'NOT LIKE', 'LIKE', 'IS NULL', 'IS NOT NULL'));

  protected $_empty_query_parts = array(
      'insert' => array(),
      'value' => array(),
      'values' => array(),
      'fields' => array(),
      'update' => array(),
      'set' => array(),
      'delete' => array(),
      'select' => array(),
      'returning' => array(),
      'from' => array(),
      'join' => array(),
      'where' => array(),
      'group_by' => array(),
      'having' => array(),
      'order_by' => array(),
      'limit' => array(),
      'offset' => array(),
      'sub_query_sql' => array(),

  );
  protected $_query_parts = array();
  protected $_sql_query_parts = array();

  protected $_tables = array();
  protected $_tables_in_use = array();
  protected $_fetch_type = \PDO::FETCH_ASSOC;
  protected $_sql = '';
  protected $_query_debug = false;
  protected $_parse_method = '';
  protected $_has_one_to_many_assoc = false;
  protected $_use_limit_subquery_in_from = true;
  protected $_use_orderby_subquery_in_from = true;
  protected $_its_count_query = false;
  protected $_count_query_sql = '';

  protected $_out = array();
  protected $_used_results_for_assoc = array();
  protected $_join_node_keys = array('local'=>array(), 'foreign'=>array());
  protected $_join_pairs = array();
  protected $_result_key_map = array();
  protected $_root_key = null;
  protected $_root_table = null;
  protected $_table_map = array();
  protected $_used_assoc = array();
  protected $_additional_select = array();


  protected $_UPDATE = array();
  protected $_SET = array();
  protected $_INSERT = array();
  protected $_VALUE = array();
  protected $_VALUES = array();
  protected $_FIELDS = array();
  protected $_DELETE = array();
  protected $_SELECT = array();
  protected $_RETURNING = array();
  protected $_FROM = array();
  protected $_WHERE = array();
  protected $_JOIN = array();
  protected $_GROUP_BY = array();
  protected $_HAVING = array();
  protected $_ORDER_BY = array();
  protected $_LIMIT = array();
  protected $_OFFSET = array();

  protected $_BIND_PARAMS = array();
  protected $_BIND_WHERE_PARAMS = array();

  protected $results = array();
  protected $_parse_result = true;
  protected $_in_transaction = false;
  protected $_no_behavior = false;

  protected $pdo_error_message = array();

//  protected function _setOptions($options){
//    $_options = array();
//    foreach(self::$_options as $k=>$v){
//      if(isset($options[$k])){
//        $_options[$k] = $options[$k];
//      }else{
//        $_options[$k] = $v;
//      }
//    }
//    return $_options;
//  }
  /**
   * query config
   */

  final public function  __construct() {
    $this->_query_parts = $this->_empty_query_parts;
    $this->_sql_query_parts = $this->_empty_query_parts;
  }
  final public function setDbh($dbh){
    $this->_dbh = $dbh;
    return $this;
  }

  final public function setTablePath($path){
    $this->_table_path = $path;
    return $this;
  }

  final public function setTableNamespace($namespace){
    $this->_table_namespace = $namespace;
    return $this;
  }

  final public function setSql($sql){
    $this->_sql = $sql;
    return $this;
  }
  final public function noBehavior(){
    $this->_no_behavior = true;
    return $this;
  }
  final public function withBehavior(){
    $this->_no_behavior = false;
    return $this;
  }
  final public function useBehavior(){
    return !$this->_no_behavior;
  }

  final public function resetBindParam(){
    $this->_BIND_PARAMS = array();
  }

  public function getCountQuerySql() {
    return $this->_count_query_sql;
  }
  public function getParseMethod() {
    return $this->_parse_method;
  }

  public function setParseMethod($parse_method) {
    $this->_parse_method = $parse_method;
  }
  public function resetParseMethod() {
    $this->_parse_method = '';
  }


  /**
   *
   * @param string|mixed $param
   * @example $param example array('type'=>'int', 'value'=>37);
   */
  final public function setBindParam($param){
    if(!\is_array($param)){
      $param = array('type' => 'string', 'value' => $param);
    }

    $this->_BIND_PARAMS[] = $param;
  }
  final public function getSql(){
    return $this->_sql;
  }

  public function isInTransaction(){
    return $this->_dbh->inTransaction();
    //return $this->_in_transaction;
  }
  public function begin(){
    $r = $this->_dbh->beginTransaction();
    if($r){
      $this->_in_transaction = true;
      return $this;
    }
    throw new \Exception('Error begining transaction.');
    return null;
  }

  public function commit(){
    $this->_in_transaction = false;
    return $this->_dbh->commit();
  }

  public function rollback(){
    $this->_in_transaction = false;
    return $this->_dbh->rollBack();
  }

  public function Count($sql = ''){
    if($sql){
      $this->setSql($sql);
    }else{
      // for behavior start
      //$this->getRootTable()->behave('DalSelect', $this);

      // for behavior end

      $_select = $this->_query_parts['select'];
      $_order_by = $this->_query_parts['order_by'];
      $_offset = $this->_query_parts['offset'];
      $_limit = $this->_query_parts['limit'];

//      $this->_query_parts['select'] = array();
//      $this->select('count(*)');
      $this->_query_parts['order_by'] = array();
      $this->_query_parts['offset'] = array();
      $this->_query_parts['limit'] = array();

      $this->_its_count_query = true;

//      \hat\dbg::alert($this->_query_parts['select']);
//      \hat\dbg::alert($this->_SELECT);

      if(empty($this->_sql) && (!$this->_prepareQueryParts() || !$this->_parseQueryParts())){
        $this->pdo_error_message[] = 'No sql.';
        return false;
      }

//      \hat\dbg::alert($this->_query_parts['select']);
//      \hat\dbg::alert($this->_SELECT);
//      //\hat\dbg::alert($this->_sql, true);
//      \hat\dbg::alert($this->_sql);
      //$this->_sql = 'SELECT count(DISTINCT t.id) FROM tree_test AS t LEFT JOIN tree_test_assoc AS ta ON t.id=ta.node_id WHERE 137=137 and ta.node_id < 76';

      // restore sql parts
      $this->_query_parts['select'] = $_select;
      $this->_query_parts['order_by'] = $_order_by;
      $this->_query_parts['offset'] = $_offset;
      $this->_query_parts['limit'] = $_limit;
    }

//    if($this->_FROM[0]['model_name'] == 'HatContentTypeLanguage'){
//      \hat\dbg::alert($this->_SELECT);
//      \hat\dbg::alert($this->_FROM);
//      \hat\dbg::alert($this->_BIND_PARAMS);
//      \hat\dbg::alert($this->_BIND_WHERE_PARAMS);
//      \hat\dbg::alert(\array_keys($this->_tables));
//      unset($this->_tables_in_use[1]);
//      unset($this->_tables_in_use[2]);
//      \hat\dbg::alert($this->_tables_in_use);
//      \hat\dbg::alert($this->_sql);
//      \hat\dbg::alert($this->_query_parts);
//      //\hat\dbg::alert($this->queryDebug(), true);
//      ///\hat\dbg::alert('kraj', true);
//      //\hat\dbg::alert($this->queryDebug());
////SELECT ctl.ct_id AS ctl__ct_id, ctl.language_id AS ctl__language_id, ctl.ct_id AS ctl__ct_id, ctl.language_id AS ctl__language_id, ctl.site_id AS ctl__site_id, ctl.name AS ctl__name FROM hat_content_type_language AS ctl WHERE (ctl.ct_id < ? AND ctl.language_id = ? AND ctl.site_id = ?) ORDER BY ctl.name COLLATE "sr_RS" DESC, ctl.ct_id ASC LIMIT 3
////SELECT ctl.ct_id AS ctl__ct_id, ctl.language_id AS ctl__language_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.ct_id AS ctl__ct_id, ctl.language_id AS ctl__language_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.name AS ctl__name FROM hat_content_type_language AS ctl WHERE (ctl.ct_id < 10 AND ctl.language_id = 1 AND ctl.site_id = 2) ORDER BY ctl.name COLLATE "sr_RS" DESC, ctl.ct_id ASC LIMIT 3
//
//    }

//    $dbg = array(
//        'where condition' => $this->_WHERE,
//        'where params' => $this->_BIND_WHERE_PARAMS,
//        'generated SQL' => $this->_sql
//    );
//      print_r($dbg);
      //print_r('end'); exit;
    $stmt = $this->_dbh->prepare($this->_sql);
//    \hat\dbg::alert($stmt->fetch());
//    \hat\dbg::alert($stmt->errorInfo());
//    \hat\dbg::alert($this->execSql($this->_sql));
//    \hat\dbg::alert($this->querySql($this->_sql));
//    \hat\dbg::alert($this->results);
//    \hat\dbg::alert(\hat\dbg::memory());
//    \hat\dbg::timer();

//    $a = clone($this);
//    $a->reset();
//    \hat\dbg::alert($a->queryStmt($this->_sql));
//    \hat\dbg::alert($a->getResultsDbRow());
//    \hat\dbg::alert(\hat\dbg::memory());
//    \hat\dbg::alert($this->_BIND_PARAMS);
//    \hat\dbg::alert($this->_BIND_WHERE_PARAMS);
////    \hat\dbg::alert();
//    \hat\dbg::timer();
//    \hat\dbg::alert('kraj', true);
    //print_r($stmt); exit;
    $i=0;
    foreach($this->_BIND_PARAMS as $k => $param){
      $i++;
      $param_type = $this->_param_type($param['type']);
      $stmt->bindParam($i, $param['value'], $param_type);
    }
    foreach($this->_BIND_WHERE_PARAMS as $j => $param){
      $i++;
      $param_type = $this->_param_type($param['type']);
      $stmt->bindParam($i, $param['value'], $param_type);
    }
    //$this->test(); exit;
    try {
      $stmt->execute();
    } catch (\Exception $e) {
      $this->pdo_error_message[] = $e->getMessage();
      return false;
    }

//    \hat\dbg::alert($this->_BIND_PARAMS);
//    \hat\dbg::alert($this->_BIND_WHERE_PARAMS);
//    \hat\dbg::alert($stmt->errorInfo());
    $results = $stmt->fetchAll($this->_fetch_type);
//    \hat\dbg::alert($stmt->errorInfo());
//    if($this->_FROM[0]['model_name'] == 'HatContentTypeLanguage'){
//      \hat\dbg::alert($results);
//      \hat\dbg::alert($this->_fetch_type);
//      \hat\dbg::alert($stmt);
//    }
//    print_r($results); exit;
//    \hat\dbg::alert($this->_sql);
//    \hat\dbg::alert($results);

    $count = 0;
    if(isset($results[0]) && isset($results[0]['count_query'])){
      $count = (int)$results[0]['count_query'];
    }

    $this->_count_query_sql = $this->_sql;
    $this->reset(false);
    return $count;

    $this->results = $results;
    return \count($results);
    return 'not implemented';

  }
  final public function getBind(){
    return array(
      'BIND_PARAMS' => $this->_BIND_PARAMS,
      'BIND_WHERE_PARAMS' => $this->_BIND_WHERE_PARAMS
    );
  }
  final public function getCount(){
    // count($this->results) will return wrong count in 'hasMany' situation.
    return 'not implemented';
    return count($this->results);
  }

  final public function getRowResults(){
    $results = $this->results;
    return $results;
  }

  final public function getResultsArray($parse_tree = true){
    return $this->getResults(\hatwebtech\dal\DAL::AS_ARRAY, $parse_tree);
  }

  final public function getResultsObject($parse_tree = true){
    return $this->getResults(\hatwebtech\dal\DAL::AS_OBJECT, $parse_tree);
  }

  final public function getResultsDbRow(){
    return $this->getResults(\hatwebtech\dal\DAL::AS_DB_ROW);
  }

  final public function getResults($as = \hatwebtech\dal\DAL::AS_ARRAY, $parse_tree = true){

//    print_r($this->_tables_in_use);
//    print_r($this->_tables); exit;
//    print_r($this->_parse_result);
//    print_r($this->results); exit;
//    print_r($this->_root_key);
//    print_r($this->_table_map);
//    print_r($this->_result_key_map); exit;
//    print_r($this->_join_node_keys);
//    print_r($this->_join_pairs);
//    print_r($this->_used_results_for_assoc);
//    print_r($this->_parse_result);
//    print_r('end'); exit;
    if(empty($this->results)){
      return null;
    }

    if($as == \hatwebtech\dal\DAL::AS_DB_ROW){
      return $this->results;
    }

    if($this->_parse_result){
      $this->_parseResult($parse_tree);
    }
    if($as == \hatwebtech\dal\DAL::AS_ARRAY){
      return $this->_out;
    }

    if($as == \hatwebtech\dal\DAL::AS_OBJECT){
      $resultObject = $this->_getRootTable();
      $results = array();
      foreach($this->_out as $out){
        $clone = clone $resultObject;
        $clone->setResults($out);
        $clone->setResultQuery($this);
        $results[] = $clone;
      }
      return $results;
      //return $this->_out;
    }

    throw new \Exception('Unknown result output type.');
  }

  protected function _parseResult($parse_tree = true){
    //print_r($this->results); exit;
    $this->_parse_result = false;
    $this->_parse_method = 'hash';

    if($this->_parse_method == ''){
      $this->_extractResultKeyMap();
      if(isset($this->results[0]) && !empty($this->results[0])){
        if(!isset($this->_root_key)){
          $this->_root_key = \key(\current($this->results));
        }
        foreach($this->results as $result_index => $result){
          if(!isset($this->_used_results_for_assoc[$result_index])){
            $this->_out[] = $this->_getAssocResult($this->_root_key, $result_index);
          }

        }
      }
    }elseif($this->_parse_method == 'hash'){

      $this->_extractResultKeyMap(false);
      //print_r($this->results[0]);
      //exit("aqw\n");
      //$this->_prepare_hash();
      $this->_result_distinction();
      if(isset($this->results[0]) && !empty($this->results[0])){
        if(!isset($this->_root_table) || empty($this->_root_table)){
          throw new \Exception("Missing or empty root_table!");
          return;
        }
        if(!isset($this->_root_key)){
          if(isset($this->results[0])){
            $this->_root_key = \key($this->results[0]);
          }else{
            trigger_error('Query result problem (no 0 element in results array)', \E_USER_ERROR);
          }
        }

        //print_r($this->_root_table); exit;

        if(isset($this->_hash_map_r[$this->_root_table['model_alias']])){
          foreach($this->_hash_map_r[$this->_root_table['model_alias']] as $result_index => $hash){
            $this->_out[] = $this->_parseAssocResult($this->_root_key, $result_index);
          }
        }
      }
    }else{
      throw new \Exception("Invalid parse result method [{$this->_parse_method}]!");
      return;
    }

    if($parse_tree && $this->_getRootTable()->isTree()){
      $this->_out = $this->_parseTree();
    }


    return $this->_out;
  }

  protected function _parseTree(){
    $trees = array();
    $l = 0;
    if(count($this->_out) > 0){
      // Node Stack. Used to help building the hierarchy
      $stack = array();
      foreach($this->_out as $child){
        $item = $child;
        $item['__children'] = array();
        // Number of stack items
        $l = count($stack);
        // Check if we're dealing with different levels
//        if(!isset($stack[$l - 1]['level'])){
//          \hat\dbg::alert($stack[$l - 1]);
//          \hat\dbg::alert($this->_out, true);
//        }
        if(isset($stack[$l - 1]) && isset($stack[$l - 1]['level'])){
          while($l > 0 && $stack[$l - 1]['level'] >= $item['level']){
            array_pop($stack);
            $l--;
          }
        }

        // Stack is empty (we are inspecting the root)
        if ($l == 0) {
          // Assigning the root child
          $i = count($trees);
          $trees[$i] = $item;
          $stack[] = & $trees[$i];
          //\hat\dbg::alert($stack);
        } else {
          // Add child to parent
          $i = count($stack[$l - 1]['__children']);
          $stack[$l - 1]['__children'][$i] = $item;
          $stack[] = & $stack[$l - 1]['__children'][$i];
          //\hat\dbg::alert($stack);
        }
      }
    }
    return $trees;
  }

//  protected function _getAssocResult_new($field, $result_index){
//
//    \hat\dbg::timmer('_getAssocResult');
//
//    //print_r(\compact('field', $result_index)); exit;
//    $this->_used_results_for_assoc[$result_index][] = $field .'_'. $this->results[$result_index][$field];
//    $result = array();
//    //$result['__/\__'] = "field : $field";
//
//    // this model filds
//    foreach($this->_table_map[$this->_result_key_map[$field]['table_alias']] as $rf){
//      $result[$this->_result_key_map[$rf]['field_name']] = $this->results[$result_index][$rf];
//    }
//
//    // associations
//    foreach($this->_table_map[$this->_result_key_map[$field]['table_alias']] as $rf){
//
//      foreach($this->_result_key_map[$rf]['local_key_for']['hasMany'] as $assoc_rf => $assoc_info){
//        foreach($this->_table_map[$this->_result_key_map[$assoc_rf]['table_alias']] as $rff){
//          //if(!isset($this->_used_assoc[$rff]) || !\in_array($this->results[$result_index][$rff], $this->_used_assoc[$rff])){
//          if(!isset($this->_used_assoc[$rf]) || !\in_array($this->results[$result_index][$rf], $this->_used_assoc[$rf])){
//            $dd = array();
////            $rff_value = $this->results[$result_index][$rff];
////            $rf_value = $this->results[$result_index][$rf];
////            $dd = \compact('rf', 'assoc_rf', 'rff','result_index', 'rf_value', 'rff_value');
//            //$dd = array('_OK_' => $rf);
//            //$dd['_OK_'] = $rf;
//
//            //$result['_ok_'] = $dd;
//          }else{
//            $dd = array();
////            $rff_value = $this->results[$result_index][$rff];
////            $rf_value = $this->results[$result_index][$rf];
//            //$dd = \compact('rf', 'assoc_rf', 'rff','result_index', 'rf_value', 'rff_value');
//            $dd['_duplicate_'] = $rf;
//
//            $result['_duplicate_'] = $dd;
//
//          }
////          if(!isset($this->_used_assoc[$rf]) || !\in_array($assoc_rf .'_/\_'. $this->results[$result_index][$rf], $this->_used_assoc[$rf])){
////
////          }else{
////            $result['_duplicate2_'] = $assoc_rf .'_/\_'. $this->results[$result_index][$rf];
////
////          }
//            //\hat\dbg::alert($dd);
//        }
//
//        //$result['_: ' . $assoc_rf . '_1'] = '1. foreach';
//      }
//
//
////      foreach($this->_result_key_map[$rf]['local_key_for']['hasOne'] as $assoc_rf => $assoc_info){
////        foreach($this->_table_map[$this->_result_key_map[$assoc_rf]['table_alias']] as $rff){
////          //if(!isset($this->_used_assoc[$rff]) || !\in_array($this->results[$result_index][$rff], $this->_used_assoc[$rff])){
//////          if(!isset($this->_used_assoc[$rf]) || !\in_array($this->results[$result_index][$rf], $this->_used_assoc[$rf])){
//////            $rff_value = $this->results[$result_index][$rff];
//////            $rf_value = $this->results[$result_index][$rf];
//////            $dd = \compact('rf', 'assoc_rf', 'rff','result_index', 'rf_value', 'rff_value');
//////            //$dd = array('_OK_' => $rf);
//////            $dd['_OK_'] = $rf;
//////
//////            //$result['_ok_'] = $dd;
//////          }else{
//////            $rff_value = $this->results[$result_index][$rff];
//////            $rf_value = $this->results[$result_index][$rf];
//////            $dd = array();
//////            //$dd = \compact('rf', 'assoc_rf', 'rff','result_index', 'rf_value', 'rff_value');
//////            $dd['_duplicate_'] = $rf;
//////
//////            $result['_duplicate_'] = $dd;
//////
//////          }
////          if(!isset($this->_used_assoc[$rf]) || !\in_array($assoc_rf .'_/\_'. $this->results[$result_index][$rf], $this->_used_assoc[$rf])){
////
////          }else{
////            $result['_duplicate2_'] = $assoc_rf .'_/\_'. $this->results[$result_index][$rf];
////
////          }
////            //\hat\dbg::alert($dd);
////        }
////
////        //$result['_: ' . $assoc_rf . '_1.5'] = '1.5. foreach';
////      }
//
//      foreach($this->_result_key_map[$rf]['local_key_for']['hasOne'] as $assoc_rf => $assoc_info){
//        if(isset($assoc_info['on_index'][$this->results[$result_index][$rf]])){
//          foreach($assoc_info['on_index'][$this->results[$result_index][$rf]] as $assoc_type => $assoc_index_keys){
//            if($assoc_type == 'hasOne'){
//                $tmp = $this->_getAssocResult($assoc_rf, $assoc_index_keys[0]);
//                $this->_used_assoc[$rf][] = $this->results[$result_index][$rf];
//                //$this->_used_assoc[$rf][] = $assoc_rf .'_/\_'. $this->results[$result_index][$rf];
//                //$tmp['__type__'] = 'hasOne';
//                if(isset($tmp['_duplicate_']) || isset($result['_duplicate_'])){
//
//                }else{
//                  $result[$assoc_info['association_alias']] = $tmp;
//                }
//                //$result[$assoc_info['association_alias']] = $tmp;
//
////              foreach($assoc_index_keys as $assoc_index_key){
////                $result[$assoc_info['association_alias']][] = $this->_getAssocResult($assoc_rf, $assoc_index_key);
////              }
//            }else{
//              throw new \Exception("Invalid assoc type in \$this->_result_key_map['$rf']['local_key_for']['hasOne'][$assoc_rf]['on_index'][{$this->results[$result_index][$rf]}]['$assoc_type']! ");
//            }
//          }
//        }
//
//        //$result['_: ' . $assoc_rf . '_2'] = '2. foreach';
//      }
//
//      foreach($this->_result_key_map[$rf]['local_key_for']['hasMany'] as $assoc_rf => $assoc_info){
//        if(isset($assoc_info['on_index'][$this->results[$result_index][$rf]])){
//          foreach($assoc_info['on_index'][$this->results[$result_index][$rf]] as $assoc_type => $assoc_index_keys){
//            if($assoc_type == 'hasMany'){
//              foreach($assoc_index_keys as $assoc_index_key){
//
//
//                  //if(!isset($this->_used_assoc[$rf]) || !\in_array($assoc_rf .'_/\_'. $this->results[$result_index][$rf], $this->_used_assoc[$rf])){
////                $_duplicate_3 = false;
////                  if(isset($this->_used_assoc[$rf]) && \in_array($assoc_rf .'_/\_'. $this->results[$result_index][$rf], $this->_used_assoc[$rf])){
////                    $_duplicate_3 = true;
////                    //$result['_duplicate_3'] = '----  ' . $assoc_rf .'_/\_'. $this->results[$result_index][$rf];
////                  }
//
//                  $tmp = $this->_getAssocResult($assoc_rf, $assoc_index_key);
//                  $this->_used_assoc[$rf][] = $this->results[$result_index][$rf];
////                  $this->_used_assoc[$rf][] = $assoc_rf .'_/\_'. $this->results[$result_index][$rf];
//                  //$used_keys = $this->_used_assoc;
//            //$rff_value = $this->results[$result_index][$rff];
////            $rf_value = $this->results[$result_index][$rf];
//            //$dd = \compact('rf', 'assoc_rf', 'rff','result_index', 'rf_value', 'rff_value');
////                  $tmp = \array_merge(array('__info__' => \compact('field', 'rf', 'rf_value', 'assoc_rf', 'assoc_index_key', 'used_keys')), $tmp);
////                  //$tmp['__type__'] = 'hasMany';
////                  if($_duplicate_3){
////                    $tmp['__duplicate_3'] = '----  ' . $assoc_rf .'_/\_'. $this->results[$result_index][$rf];
////                  }
//
//                  if(isset($tmp['_duplicate_'])){
//                  //if(isset($tmp['_duplicate_']) || isset($result['_duplicate_'])){
//                    // skip duplicate
//                    //unset($tmp['_dd_']);
//                    //$result[$assoc_info['association_alias']][] = $tmp;
//                  }else{
//                    $_duplicate = false;
////                    if(isset($tmp['_duplicate_'])){
////                      $_duplicate = $tmp['_duplicate_'];
////                      \hat\dbg::alert('____duplicate 1');
////                    }
////                    if(isset($result['_duplicate_'])){
////                      $_duplicate = $result['_duplicate_'];
////                      //\hat\dbg::alert('____duplicate 2');
////                    }
//                    //$result[$assoc_info['association_alias']][] = $tmp;
//                    if($_duplicate){
//                      $result['_duplicate_'] = $_duplicate;
//                    }else{
//                      $result[$assoc_info['association_alias']][] = $tmp;
//                    }
//                  }
//              }
//
//            }else{
//              throw new \Exception("Invalid assoc type in \$this->_result_key_map['$rf']['local_key_for']['hasMany'][$assoc_rf]['on_index'][{$this->results[$result_index][$rf]}]['$assoc_type']! ");
//            }
//          }
//        }
//
//        //$result['_: ' . $assoc_rf . '_3'] = '3. foreach';
//      }
//
//    }
//
//    return $result;
//
//  }

  protected function _parseAssocResult($field, $result_index, $type = ''){
    //echo " eureka ($field, $result_index) <br/>\n";
    $result = array();

    // this model filds
    if(!isset($this->_result_key_map[$field]) || !isset($this->_table_map[$this->_result_key_map[$field]['table_alias']])){
      //\hat\dbg::alert(compact(array('field', 'result_index', 'type')));
      //\hat\dbg::alert($this->_result_key_map);
      //\hat\dbg::alert($this->_tables_in_use);
      //\hat\dbg::alert($this->_table_map, true);
    }
    foreach($this->_table_map[$this->_result_key_map[$field]['table_alias']] as $rf){
      $result[$this->_result_key_map[$rf]['field_name']] = $this->results[$result_index][$rf];
    }

    // associations
//    \hat\dbg::alert("$type -> field: $field - #$result_index (Loop => {$this->_result_key_map[$field]['model_name']}[ ] )\t\t''2");
    foreach($this->_table_map[$this->_result_key_map[$field]['table_alias']] as $ii => $rf){
//      \hat\dbg::alert(" ___ #$ii = $type -> field: $field - #$result_index ( {$this->_result_key_map[$field]['table_alias']}[ $rf <-\$rf ] )\t\t''3");
      foreach($this->_result_key_map[$rf]['local_key_for']['hasOne'] as $assoc_rf => $assoc_info){
//          $dbg = array();
//          $dbg['field'] = $field;
//          $dbg['result_index'] = $result_index;
//          $dbg['assoc_rf'] = $assoc_rf;
//          $dbg['rf'] = $rf;
//          $dbg['assoc_info'] = $assoc_info;
        foreach($this->_hash_map_r[$assoc_info['association_table_alias']] as $_result_index => $hash){
          if($this->results[$result_index][$rf] == $this->results[$_result_index][$assoc_rf]){
            //\hat\dbg::alert("hasOne ({$this->_result_key_map[$field]['table_alias']}[$rf] = {$this->results[$result_index][$rf]}) {$assoc_info['local_key']} #$_result_index");
//            $dbg['___EUREKA___' . $_result_index] = $_result_index;
            $result[$assoc_info['association_alias']] = $this->_parseAssocResult($assoc_info['foreign_key'], $_result_index, 'o');
            //$result[$assoc_info['association_alias']] = $this->_parseAssocResult($assoc_info['local_key'], $_result_index, 'o');
            //$result[$assoc_info['association_alias']] = $this->_parseAssocResult($assoc_rf, $_result_index, 'o');
//            $dbg['___EUREKA___' . $_result_index . '__RESULT'] = $result[$assoc_info['association_alias']];
          }
        }

        //\hat\dbg::alert($dbg);
      }

      foreach($this->_result_key_map[$rf]['local_key_for']['hasMany'] as $assoc_rf => $assoc_info){
//          $dbg = array();
//          $dbg['field'] = $field;
//          $dbg['result_index'] = $result_index;
//          $dbg['assoc_rf'] = $assoc_rf;
//          $dbg['rf'] = $rf;
//          $dbg['assoc_info'] = $assoc_info;
        foreach($this->_hash_map_r[$assoc_info['association_table_alias']] as $_result_index => $hash){
          //echo "<br> IF([$result_index][$field] == [$_result_index][$assoc_rf]) ===> ";
//          echo "<br> IF([$result_index][$rf] == [$_result_index][$assoc_rf]) ===> ";
          //if($this->results[$result_index][$field] == $this->results[$_result_index][$assoc_rf]){
          if($this->results[$result_index][$rf] == $this->results[$_result_index][$assoc_rf]){
//            echo "YES! <br>";
            //\hat\dbg::alert("hasMany ({$this->_result_key_map[$field]['table_alias']}[$rf] = {$this->results[$result_index][$rf]}) {$assoc_info['local_key']} #$_result_index");
//            \hat\dbg::alert("hasMany -  [$result_index][$field] == [$_result_index][$assoc_rf]");
//            $dbg['___EUREKA___' . $_result_index] = "_parseAssocResult($assoc_rf, $_result_index, 'm')";
//        \hat\dbg::alert($dbg);
            //$result[$assoc_info['association_alias']][] = $this->_parseAssocResult($assoc_info['local_key'], $_result_index, 'm');
            $result[$assoc_info['association_alias']][] = $this->_parseAssocResult($assoc_rf, $_result_index, 'm');
          }else{
//            echo "no <br>";
          }
        }
        if(isset($result[$assoc_info['association_alias']])){
//          $dbg['assoc_count'] = count($result[$assoc_info['association_alias']]);
//          \hat\dbg::alert("assoc_count for [{$assoc_info['association_alias']}] = {$dbg['assoc_count']}");
        }

        //\hat\dbg::alert($dbg);
      }
    }
//    echo "<br>------ end 2'' ------- $type -> field: $field - #$result_index ( {$this->_result_key_map[$field]['model_name']}[ ] )<br>\n\n";

    return $result;
  }

  protected $_hash_results = array();
  protected $_hash_results_r = array();
  protected $_hash_map = array();
  protected $_hash_map_r = array();

  protected function _prepare_hash(){
    // hash('md5', 'string');
    $algo = 'md5';
    $dbg = array();
    $dbg['root_key'] = $this->_root_key;
    $dbg['root_table'] = $this->_root_table;
    $dbg['tables_map'] = $this->_table_map;
    $dbg['tables_map_primary'] = $this->_table_map_primary;
    $dbg['tables_in_use'] = $this->_tables_in_use;
    $dbg['result_key_map__keys'] = array_keys($this->_result_key_map);
    $dbg['result_key_map'] = $this->_result_key_map;

    //\hat\dbg::timmer('hashing');
    foreach($this->results as $k => $r){
//      $this->_hash_results[$k] = array();
//      $this->_hash_results_r[$k] = array();
      foreach($this->_table_map as $alias => $fields){
        $for_hash = '';
        foreach($fields as $field){
          $for_hash .= $r[$field];
        }
        $hash = hash($algo, $for_hash);
//        $this->_hash_results[$k][$alias] = $hash;
//        $this->_hash_results_r[$k][$hash] = $alias;
//        //$this->_hash_map[$hash][$k] = $alias;
//        $this->_hash_map[$hash]['alias'] = $alias;
//        $this->_hash_map[$hash]['index'] = $k;

        if(!isset($this->_hash_map_r[$alias])){
          $this->_hash_map_r[$alias] = array();
        }
        if(!in_array($hash, $this->_hash_map_r[$alias])){
          $this->_hash_map_r[$alias][$k] = $hash;
        }
      }
    }
    //\hat\dbg::timmer('hashing');

//    $dbg['hash_count'] = count($this->_hash_map);
//    $dbg['hash_map'] = $this->_hash_map;
    $dbg['hash_map_r'] = $this->_hash_map_r;
//    $dbg['hash_results'] = $this->_hash_results;
//    $dbg['hash_results_r'] = $this->_hash_results_r;
//    $dbg['row_results'][] = $this->results[0];
//    $dbg['row_results'][] = $this->results[1];
    //\hat\dbg::alert($dbg);

  }

  protected $_distinction_map = array();
  protected function _result_distinction(){
    $dbg = array();
    //$dbg['sql'] = $this->queryDebug();
//    $dbg['root_key'] = $this->_root_key;
//    $dbg['root_table'] = $this->_root_table;
//    $dbg['table_map'] = $this->_table_map;
//    $dbg['tables_in_use'] = $this->_tables_in_use;
//    $dbg['result_key_map__keys'] = array_keys($this->_result_key_map);
//    $dbg['result_key_map'] = $this->_result_key_map;

//    \hat\dbg::timmer('distinction');
    foreach($this->results as $k => $r){
      foreach($this->_table_map_primary as $alias => $fields){
        $for_hash = array();
        foreach($fields as $field){
          $for_hash[] = $r[$field];
        }
        $hash = implode('_', $for_hash);
//        $this->_hash_results[$k][$alias] = $hash;
//        $this->_hash_results_r[$k][$hash] = $alias;
//        //$this->_hash_map[$hash][$k] = $alias;
//        $this->_hash_map[$hash]['alias'] = $alias;
//        $this->_hash_map[$hash]['index'] = $k;

        if(!isset($this->_hash_map_r[$alias])){
          $this->_hash_map_r[$alias] = array();
        }
        if(!in_array($hash, $this->_hash_map_r[$alias])){
          $this->_hash_map_r[$alias][$k] = $hash;
        }
      }
    }
//    \hat\dbg::timmer('distinction');

//    $dbg['hash_count'] = count($this->_hash_map);
//    $dbg['hash_map'] = $this->_hash_map;
//    $dbg['hash_map_r'] = $this->_hash_map_r;
//    $dbg['hash_results'] = $this->_hash_results;
//    $dbg['hash_results_r'] = $this->_hash_results_r;
//    //$dbg['row_results'] = $this->results;
//    $dbg['row_results'][] = $this->results[0];
//    $dbg['row_results'][] = $this->results[1];
//    print_r($dbg); exit;
//    \hat\dbg::alert($dbg);
    //\hat\dbg::alert('kraj', true);


  }

  protected function _getAssocResult($field, $result_index){
//    \hat\dbg::alert('_getAssocResult');
//    if($this->_query_debug2){
//      //\hat\dbg::timmer('_getAssocResult');
//      echo " eureka <br/>\n";
//    }

    //print_r(\compact('field', $result_index)); exit;
    $this->_used_results_for_assoc[$result_index][] = $field .'_'. $this->results[$result_index][$field];
    $result = array();

    // this model filds
    foreach($this->_table_map[$this->_result_key_map[$field]['table_alias']] as $rf){
      $result[$this->_result_key_map[$rf]['field_name']] = $this->results[$result_index][$rf];
    }

    // associations
    foreach($this->_table_map[$this->_result_key_map[$field]['table_alias']] as $rf){

      foreach($this->_result_key_map[$rf]['local_key_for']['hasMany'] as $assoc_rf => $assoc_info){
        foreach($this->_table_map[$this->_result_key_map[$assoc_rf]['table_alias']] as $rff){
          //if(!isset($this->_used_assoc[$rff]) || !\in_array($this->results[$result_index][$rff], $this->_used_assoc[$rff])){
          if(!isset($this->_used_assoc[$rf]) || !\in_array($this->results[$result_index][$rf], $this->_used_assoc[$rf])){
            $dd = array('_OK_' => $rf);
            //$result['_ok_'] = $dd;
          }else{
            $rff_value = $this->results[$result_index][$rff];
            $rf_value = $this->results[$result_index][$rf];
            $dd = \compact('rf', 'assoc_rf', 'rff','result_index', 'rf_value', 'rff_value');
            $dd = array('_duplicate_' => $rf);
            $result['_duplicate_'] = $dd;

          }
        }
      }

      foreach($this->_result_key_map[$rf]['local_key_for']['hasOne'] as $assoc_rf => $assoc_info){
        if(isset($assoc_info['on_index'][$this->results[$result_index][$rf]])){
          foreach($assoc_info['on_index'][$this->results[$result_index][$rf]] as $assoc_type => $assoc_index_keys){
            if($assoc_type == 'hasOne'){
                $tmp = $this->_getAssocResult($assoc_rf, $assoc_index_keys[0]);
                  $result[$assoc_info['association_alias']] = $tmp;
//              foreach($assoc_index_keys as $assoc_index_key){
//                $result[$assoc_info['association_alias']][] = $this->_getAssocResult($assoc_rf, $assoc_index_key);
//              }
            }else{
              throw new \Exception("Invalid assoc type in \$this->_result_key_map['$rf']['local_key_for']['hasOne'][$assoc_rf]['on_index'][{$this->results[$result_index][$rf]}]['$assoc_type']! ");
            }
          }
        }

      }

      foreach($this->_result_key_map[$rf]['local_key_for']['hasMany'] as $assoc_rf => $assoc_info){
        if(isset($assoc_info['on_index'][$this->results[$result_index][$rf]])){
          foreach($assoc_info['on_index'][$this->results[$result_index][$rf]] as $assoc_type => $assoc_index_keys){
            if($assoc_type == 'hasMany'){
              foreach($assoc_index_keys as $assoc_index_key){
                  $tmp = $this->_getAssocResult($assoc_rf, $assoc_index_key);
                  $this->_used_assoc[$rf][] = $this->results[$result_index][$rf];
                  //$used_keys = $this->_used_assoc;
                  //$tmp = \array_merge(array('__info__' => \compact('field', 'rf', 'assoc_rf', 'assoc_index_key', 'used_keys')), $tmp);
                  if(isset($tmp['_duplicate_'])){
                    // skip duplicate
                    //unset($tmp['_dd_']);
                    //$result[$assoc_info['association_alias']][] = $tmp;
                  }else{
                      $result[$assoc_info['association_alias']][] = $tmp;
                    }
                  }

            }else{
              throw new \Exception("Invalid assoc type in \$this->_result_key_map['$rf']['local_key_for']['hasMany'][$assoc_rf]['on_index'][{$this->results[$result_index][$rf]}]['$assoc_type']! ");
            }
          }
        }
      }

    }

    return $result;
  }

  protected $_table_map_primary = array();
  protected function _extractResultKeyMap($with_on_index = true){
    $rf_map = array(
      'model_name' => null,
      'table_alias' => null,
      'field_name' => null,
      'root_level' => false,
      'model' => array(),
      'local_key_for' => array('hasOne' => array(), 'hasMany' => array()),
      'foreign_key_for' => array('hasOne' => array(), 'hasMany' => array()),

    );
    $this->_result_key_map = \array_fill_keys(\array_keys($this->results[0]), $rf_map);
    //print_r($this->_result_key_map); exit;
    foreach($this->_result_key_map as $rf => $rf_info){
      $table_alias = $this->_getFieldTableAlias($rf);
      //$table_alias = strtolower($table_alias);
      $field = $this->_getFieldName($rf);

      $this->_result_key_map[$rf]['table_alias'] = $table_alias;
      $this->_result_key_map[$rf]['field_name'] = $field;

      if(!isset($this->_table_map[$table_alias])){
        $this->_table_map[$table_alias] = array();
      }
      if(!\in_array($rf, $this->_table_map[$table_alias])){
        $this->_table_map[$table_alias][] = $rf;
      }

      //print_r($this->_tables_in_use); exit;
      foreach($this->_tables_in_use as $tiu){

        if(isset($tiu['assoc_name'])){
          $parent_field = $tiu['assoc_to_alias'] . '__' . $tiu['assoc_info']['keys']['local'];
          $child_field = $tiu['model_alias'] . '__' . $tiu['assoc_info']['keys']['foreign'];

          if($rf == $child_field){
            // this key is connected to something
            $this->_result_key_map[$rf]['foreign_key_for'][$tiu['assoc_info']['type']][$parent_field] = $tiu['assoc_to_name'];
          }
          if($rf == $parent_field){
            // something is connected to this key
            $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['association_alias'] = $tiu['assoc_info']['association_alias'];
            $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['association_table_alias'] = $tiu['model_alias'];

//            if($tiu['model_alias'] == 'ca'){
//              \hat\dbg::alert($rf);
//              \hat\dbg::alert($rf_info);
//              \hat\dbg::alert($tiu);
//              //\hat\dbg::alert('kraj', true);
//            }
            $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['local_key'] = $tiu['model_alias'] .'__'. $tiu['assoc_info']['keys']['local'];
            $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['foreign_key'] = $tiu['model_alias'] .'__'. $tiu['assoc_info']['keys']['foreign'];

//            $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['local_key'] = $tiu['model_alias'] .'__'. $tiu['assoc_info']['keys']['foreign'];
//            $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['foreign_key'] = $tiu['assoc_to_alias'] .'__'. $tiu['assoc_info']['keys']['local'];

            $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['tiu_assoc_info'] = $tiu['assoc_info'];
            //

            $assoc_at_keys = array();
            if($with_on_index){
              foreach($this->results as $result_index => $result){
                if(isset($result[$child_field]) && isset($result[$parent_field]) && $result[$child_field] == $result[$parent_field]){
                  $assoc_at_keys[$result[$parent_field]][$tiu['assoc_info']['type']][] = $result_index;
                  //print_r(\compact('parent_field', 'child_field', 'result', 'tiu')); exit;
                }
              }
              $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['on_index'] = $assoc_at_keys;
            }else{
              foreach($this->results as $result_index => $result){
                if(isset($result[$child_field]) && isset($result[$parent_field]) && $result[$child_field] == $result[$parent_field]){
                  $assoc_at_keys[$result[$parent_field]] = $tiu['assoc_info']['type'];
                }
              }
              $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['assoc_at_keys'] = $assoc_at_keys;
//              $assoc_at_keys[$result[$parent_field]] = $tiu['assoc_info']['type'];
//              $this->_result_key_map[$rf]['local_key_for'][$tiu['assoc_info']['type']][$child_field]['assoc_at_keys'] = $assoc_at_keys;
            }

            if($this->_result_key_map[$rf]['root_level']){
              $this->_root_key = $rf;
              //$this->_root_keys[] = $rf;
            }
          }
        }

        if($tiu['model_alias'] == $table_alias){
          // this tiu is used for this rf
          $this->_result_key_map[$rf]['model_name'] = $tiu['model_name'];
          if(isset($tiu['assoc_name'])){
            if($tiu['model_alias'] == $table_alias){
              $this->_result_key_map[$rf]['model_name'] = $tiu['assoc_name'];
            }
          }else{
            $this->_result_key_map[$rf]['root_level'] = true;
            if(!empty($this->_result_key_map[$rf]['local_key_for']['hasOne']) ||
               !empty($this->_result_key_map[$rf]['local_key_for']['hasMany'])){
              $this->_root_key = $rf;
            }
          }
        }

      }

    }
    //print_r($this->_result_key_map); exit;

    foreach($this->_tables_in_use as $tiu){
      $table_alias = strtolower($tiu['model_alias']);
      if(!isset($this->_table_map_primary[$table_alias])){
        $this->_table_map_primary[$table_alias] = array();
      }
      if(!isset($tiu['primary_keys'])){
        echo "<pre>";
        //\hat\dbg::alert($tiu);
        trigger_error('missing primary_keys', \E_USER_ERROR);
        //\hat\dbg::alert($tiu,true);
      }
      foreach($tiu['primary_keys'] as $pk){
        $pk_field = $table_alias . '__' . $pk;
        if(!isset($this->_table_map[$table_alias])){
          echo "<pre>";
          //\hat\dbg::alert($this->_table_map);
          print_r($this->_tables_in_use);
          print_r($this->_table_map_primary);
          print_r($this->_table_map);
          trigger_error("missing $table_alias in \$this->_table_map", \E_USER_ERROR);
          exit;
          //\hat\dbg::alert($this->_table_map,true);

        }
        if(in_array($pk_field, $this->_table_map[$table_alias])){
          if(!\in_array($pk_field, $this->_table_map_primary[$table_alias])){
            $this->_table_map_primary[$table_alias][] = $pk_field;
          }
        }else{
//          \hat\dbg::alert($this->_table_map);
//          \hat\dbg::alert($this->_tables_in_use);
//          \hat\dbg::alert("$table_alias.$pk must appear in select", true);
          //throw new \Exception("{$tiu['model_alias']}.$pk must appear in select");
          \trigger_error("$table_alias.$pk must appear in select.", \E_USER_WARNING);
        }
      }
    }

    //print_r($this->_result_key_map);
    return true;

  }

  protected function _getFieldName($field_alias){
    $field_parts = \explode('__', $field_alias);
    $field_name = \array_pop($field_parts);

    return $field_name;
  }

  protected function _getFieldTableAlias($field_alias){
    $field_parts = \explode('__', $field_alias);
    $field_table_alias = \array_shift($field_parts);

    return $field_table_alias;
  }

  final public function querySql($sql){
    $this->setSql($sql);
    return $this->query();
  }

  /**
   *
   * @return <string> SQL query
   */
  final public function queryDebug($level = 0){
    $this->_query_debug = true;
    if($level == 1){
      //\hat\dbg::alert($this->_query_parts);
    }
    if(empty($this->_sql) && (!$this->_prepareQueryParts() || !$this->_parseQueryParts())){
      $this->pdo_error_message[] = 'No sql.';
      return 'No sql';
    }
    if($level == 1){
      //\hat\dbg::alert($this->_sql_query_parts);
    }
    if($level == 2){
      //\hat\dbg::alert(array(
      //  'BIND_PARAMS' => $this->_BIND_PARAMS,
      //  'BIND_WHERE_PARAMS' => $this->_BIND_WHERE_PARAMS,
      //));
    }

    $sql = $this->_sql;

    $this->reset(false);
    return $sql;
  }

  /**
   *
   * @return Table $result
   */
  final public function fetchOne(){
    //$this->limit(1);
    try {
      //\hat\dbg::alert($this->queryDebug());
      $r = $this->queryStmt();
      //\hat\dbg::alert($r);
      //\hat\dbg::alert($this->_out);
      //\hat\dbg::alert($this->results);
    } catch (\Exception $e) {
      $dbg['msg'] = $e->getMessage();
      // $dbg['trace'] = $e->getTraceAsString();
      $this->pdo_error_message[] = $dbg['msg'];
      //\hat\dbg::alert($dbg);
      return false;
    }
    //$r = $this->getResultsObject();
    $r = $this->getResults();
    //\hat\dbg::alert($r);
    if(isset($r[0])){
      return $r[0];
    }
    $this->pdo_error_message[] = 'Not found';
    //\hat\dbg::alert('not found');
    return false;
  }

  final public function queryStmt($sql = null){
    if($sql){
      $this->setSql($sql);
    }else{
      // for behavior start
      //$this->getRootTable()->behave('DalSelect', $this);

      // for behavior end

      if(empty($this->_sql) && (!$this->_prepareQueryParts() || !$this->_parseQueryParts())){
        $this->pdo_error_message[] = 'No sql.';
        return false;
      }
    }

//    if($this->_FROM[0]['model_name'] == 'HatContentTypeLanguage'){
//      \hat\dbg::alert($this->_SELECT);
//      \hat\dbg::alert($this->_FROM);
//      \hat\dbg::alert($this->_BIND_PARAMS);
//      \hat\dbg::alert($this->_BIND_WHERE_PARAMS);
//      \hat\dbg::alert(\array_keys($this->_tables));
//      unset($this->_tables_in_use[1]);
//      unset($this->_tables_in_use[2]);
//      \hat\dbg::alert($this->_tables_in_use);
//      \hat\dbg::alert($this->_sql);
//      \hat\dbg::alert($this->_query_parts);
//      //\hat\dbg::alert($this->queryDebug(), true);
//      ///\hat\dbg::alert('kraj', true);
//      //\hat\dbg::alert($this->queryDebug());
////SELECT ctl.ct_id AS ctl__ct_id, ctl.language_id AS ctl__language_id, ctl.ct_id AS ctl__ct_id, ctl.language_id AS ctl__language_id, ctl.site_id AS ctl__site_id, ctl.name AS ctl__name FROM hat_content_type_language AS ctl WHERE (ctl.ct_id < ? AND ctl.language_id = ? AND ctl.site_id = ?) ORDER BY ctl.name COLLATE "sr_RS" DESC, ctl.ct_id ASC LIMIT 3
////SELECT ctl.ct_id AS ctl__ct_id, ctl.language_id AS ctl__language_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.ct_id AS ctl__ct_id, ctl.language_id AS ctl__language_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.site_id AS ctl__site_id, ctl.name AS ctl__name FROM hat_content_type_language AS ctl WHERE (ctl.ct_id < 10 AND ctl.language_id = 1 AND ctl.site_id = 2) ORDER BY ctl.name COLLATE "sr_RS" DESC, ctl.ct_id ASC LIMIT 3
//
//    }

//    $dbg = array(
//        'where condition' => $this->_WHERE,
//        'where params' => $this->_BIND_WHERE_PARAMS,
//        'generated SQL' => $this->_sql
//    );
//      print_r($dbg);
      //print_r('end'); exit;
    $stmt = $this->_dbh->prepare($this->_sql);
    $i=0;
    foreach($this->_BIND_PARAMS as $k => $param){
      $i++;
      $param_type = $this->_param_type($param['type']);
      $stmt->bindParam($i, $param['value'], $param_type);
    }
    foreach($this->_BIND_WHERE_PARAMS as $j => $param){
      $i++;
      $param_type = $this->_param_type($param['type']);
      $stmt->bindParam($i, $param['value'], $param_type);
    }
    //$this->test(); exit;
    try {
      $r = $stmt->execute();
      if($r == false){
        $error_info = $stmt->errorInfo();
        $this->pdo_error_message[] = $error_info[2];
        return false;
      }
    } catch (\Exception $e) {
      $this->pdo_error_message[] = $e->getMessage();
      return false;
    }

    $results = $stmt->fetchAll($this->_fetch_type);
//    if($this->_FROM[0]['model_name'] == 'HatContentTypeLanguage'){
//      \hat\dbg::alert($results);
//      \hat\dbg::alert($this->_fetch_type);
//      \hat\dbg::alert($stmt);
//    }
//    print_r($results); exit;
    $this->results = $results;
    return \count($results);
  }

  protected function _param_type($type){
    switch(\strtolower($type)){
      case 'string' :
      case 'str' :
      case 'text' :
      case 'date' :
      case 'timestamp' :
        $param_type = \PDO::PARAM_STR;
        break;

      case 'boolean' :
      case 'bool' :
        $param_type = \PDO::PARAM_BOOL;
        break;

      case 'int' :
      case 'integer' :
        $param_type = \PDO::PARAM_INT;
        break;

      default: $param_type = \PDO::PARAM_STR;
    }

    return $param_type;
  }

  final public function query(){
    if(empty($this->_sql)){
      $this->pdo_error_message[] = 'No sql.';
      return false;
    }
    try {
      $results = $this->_dbh->query($this->_sql, $this->_fetch_type);
    } catch (\Exception $e) {
      $this->pdo_error_message[] = $e->getMessage();
      return false;
    }

    if($results){
      $this->results = $results->fetchAll($this->_fetch_type);
      return \count($this->results);
    }else{
      $err = $this->_dbh->errorInfo();
      if(isset($err[2])){
        $this->pdo_error_message[] = $err[2];
      }else{
        $this->pdo_error_message[] = 'error with query';
      }

      echo "========= \n";
      var_dump($results);
      print_r($this->_sql);
      print_r($this->_BIND_PARAMS);
      print_r($this->_BIND_WHERE_PARAMS);
      print_r($this->_dbh->errorInfo());
      exit;

      return false;
    }

    if(\method_exists($results, 'fetchAll')){
      $this->results = $results->fetchAll($this->_fetch_type);
    }else{
      echo "========= \n";
      var_dump($results);
      print_r($this->_sql);
      print_r($this->_BIND_PARAMS);
      print_r($this->_BIND_);
      print_r($this->_dbh->errorInfo());
      exit;
    }

    return \count($this->results);
  }

  final public function execSql($sql){
    $this->setSql($sql);
    return $this->exec();
  }

  final public function exec(){
    if(empty($this->_sql)){
      $this->pdo_error_message[] = 'No sql.';
      return false;
    }
    try {
      $results = $this->_dbh->exec($this->_sql);
    } catch (\Exception $e) {
      $this->pdo_error_message[] = $e->getMessage();
      return false;
    }

    return $results;
  }

  final public function delete($delete){
    $this->_query_parts['delete'] = array($delete);
    return $this;
  }

  final public function insertInto($insert){
    $this->_query_parts['insert'] = array($insert);
    return $this;
  }

  final public function value($name, $value){
    $this->_query_parts['value'][$name] = $value;
    return $this;
  }

  final public function values($values){
    $this->_query_parts['values'][] = $values;
    return $this;
  }

  final public function insertFields($fields){
    $this->_query_parts['fields'] = $fields;
    return $this;
  }

  final public function update($update){
    $this->_query_parts['update'] = array($update);
    return $this;
  }

  /**
   * @TODO enable set('views = views + ?', 1)
   */
  final public function set($set, $param){
    $this->_query_parts['set'][$set] = $param;
    return $this;
  }

  final public function select($select = '*'){
    $this->_query_parts['select'] = array($select);
    return $this;
  }

  final public function returning($returning){
    $this->_query_parts['returning'] = array($returning);
    return $this;
  }

  final public function addSelect($select = '*'){
    $this->_query_parts['select'][] = $select;
    return $this;
  }

  // TODO add 3th argument array of params
  /**
   *
   * @param string $sql
   * @param string $sub_query_name
   * @param mixed $param array(array('type'=>'int', 'value'=>$id))
   * @return DalQuery
   */
  final public function subQuerySql($sql, $sub_query_name = 'subquery', $param = null){
    $this->_query_parts['sub_query_sql'][$sub_query_name] = $sql;
    if(isset($param)){
      if(!\is_array(\current($param))){
        throw new \Exception("\$param must by array of param arrays. Example array(array('type'=>'int', 'value'=>\$id))");
        //$param = array($param);
      }
      foreach($param as $k=>$_param){
        $this->setBindParam($_param);
      }
    }
    return $this;
  }



  final public function from($from){
    $this->_query_parts['from'][] = $from;
    return $this;
  }

  /**
   * Left join tables as defined in table definition class
   *
   * $join string should be 'foo.BarAsoc bar' or 'foo.BarAsoc as bar'
   * $condition is array of aditional conditions
   *
   * @param string $join
   * @param mixed $condition
   * @return DalQuery
   *
<code>leftJoin("foo.BarAsoc bar", $condition);</code> // join BarAsoc model to foo with alias bar<br/>
where: <br/>
<ul>
   <li>
      <code>$condition = array('OR' => array('bar.id >?' => 1, 'bar.id <>?' => 3));</code><br/>
      generate:<br/>
      <code>LEFT JOIN bar_asoc AS bar ON foo.id=bar.node_id AND (bar.id > 1 OR bar.id <> 3)</code>
   </li>
   <li>
      <code>$condition = array('bar.id >?' => 1, 'bar.id <>?' => 3);</code><br/>
      generate:<br/>
      <code>LEFT JOIN bar_asoc AS bar ON foo.id=bar.node_id AND (bar.id > 1 AND bar.id <> 3)</code>
   </li>
   </ul>

   */
  final public function leftJoin($join, $condition = null){
    $this->_query_parts['join'][] = array('type' => 'left', 'join' => $join, 'condition' => $condition);
    return $this;
  }

  final public function setJoinCondition($where, $index){
    if(isset($this->_query_parts['join'][$index])){
      $this->_query_parts['join'][$index]['condition'] = $where;
    }
  }

  final public function setWhereCondition($where){
    $this->_query_parts['where'] = $where;
    return $this;
  }

  /**
   * @TODO enable != alias for <>
   */
  final public function where($where, $param){
    $this->_query_parts['where'] = array($where . ' ' => $param);
    return $this;
  }

  /**
   *
   * @param <type> $where
   * @param <type> $param
   * @return DalQuery
   * @example whereIn('id', array(1,2,3));
   */
  final public function whereIn($where, $param){
    $this->_query_parts['where'] = array($where . ' IN ?' => $param);
    return $this;
  }

  final public function whereNotIn($where, $param){
    $this->_query_parts['where'] = array($where . ' NOT IN ?' => $param);
    return $this;
  }

  final public function whereBetween($where, $param){
    $this->_query_parts['where'] = array($where . ' BETWEEN ?' => $param);
    return $this;
  }

  final public function whereNotBetween($where, $param){
    $this->_query_parts['where'] = array($where . ' NOT BETWEEN ?' => $param);
    return $this;
  }

  final public function whereLike($where, $param){
    $this->_query_parts['where'] = array($where . ' LIKE ?' => $param);
    return $this;
  }

  final public function whereNotLike($where, $param){
    $this->_query_parts['where'] = array($where . ' NOT LIKE ?' => $param);
    return $this;
  }

  final public function whereIsNull($where){
    $this->_query_parts['where'] = array($where . ' IS NULL ' => null);
    return $this;
  }

  final public function whereIsNotNull($where){
    $this->_query_parts['where'] = array($where . ' IS NOT NULL ' => null);
    return $this;
  }


  final public function andWhere($where, $params = array()){
    $_where = $this->_query_parts['where'];
    if(!\is_array($_where)){
      $_where = array($_where);
    }
    //$op = \strtoupper(\key(\current($_where)));
    $op = \strtoupper(\key($_where));
//    print_r($_where);
//    print_r($op);
    if($op != 'AND'){
      $_where[$where] = $params;
      $_tmp = $_where;
      $_where = array();
      $_where['AND'] = $_tmp;
    }else{
      $_where['AND'][$where] = $params;

    }

    $this->_query_parts['where'] = $_where;
//    print_r($_where);
    //print_r($_where); exit;
    return $this;
  }

  final public function orWhere($where, $params = array()){

    $_where = $this->_query_parts['where'];
    if(!\is_array($_where)){
      $_where = array($_where);
    }
    //$op = \strtoupper(\key(\current($_where)));
    $op = \strtoupper(\key($_where));
//    print_r($_where);
//    print_r($op);
    if($op != 'OR'){
      $_where[$where] = $params;
      $_tmp = $_where;
      $_where = array();
      $_where['OR'] = $_tmp;
    }else{
      $_where['OR'][$where] = $params;

    }

    $this->_query_parts['where'] = $_where;
//    print_r($_where);
    //print_r($_where); exit;
    return $this;
  }

  private function _contain_where($hql, $part, $exact){
    $return = false;
    //\hat\dbg::alert($part, true);
    foreach($part as $k=>$v){
      //if(\is_array($v)){
      if(\in_array(\strtoupper($k), array('AND', 'OR'))){
        $return = $return || $this->_contain_where($hql, $v, $exact);
      }else{
        if($exact){
          if($k == $hql){
            //\hat\dbg::alert(compact('part', 'hql'));
            $return = true;
          }
        }else{
          $in = \strpos($k, $hql);
          //\hat\dbg::alert(array(\compact('hql', 'k', 'v', 'in')));
          if($in === false){
            // not in
          }else{
            // in
            //\hat\dbg::alert(compact('part', 'hql'));
            $return = true;
          }
        }
      }
    }
    return $return;
  }
  private function _contain_value($hql, $part){
    $return = false;
    //\hat\dbg::alert($part, true);

    if(isset($part[$hql])){
      //\hat\dbg::alert(compact('part', 'hql'));
      $return = true;
    }

    return $return;
  }
  private function _contain_set($hql, $part){
    $return = false;
    //\hat\dbg::alert($part, true);

    if(isset($part[$hql])){
      //\hat\dbg::alert(compact('part', 'hql'));
      $return = true;
    }

    return $return;
  }

  function contains($hql, $exact = true){
    $contain = false;
    //\hat\dbg::alert($this->_query_parts);
    foreach($this->_query_parts as $part_name => $part){
      switch ($part_name) {
        case 'where':
          $contain = $this->_contain_where($hql, $part, $exact);
          break;
        case 'value':
          $contain = $this->_contain_value($hql, $part);
          break;
        case 'set':
          $contain = $this->_contain_set($hql, $part);
          break;

        default:
          break;
      }
      if($contain){
        return $contain;
      }
    }
    //\hat\dbg::alert($contain);
    return $contain;
  }

  function _where($where){

    $rr = array();
    foreach($where as $k=>$v){
      if($this->_isConditionOperand($k)){
        // subwhere
        // don't glue conditions that are for join querys! ?
        $_rr = $this->_glue($k, $this->_where($v));
        $rr[] = "($_rr)";
      }else{
        $rr[] = $this->_predicate($k, $v);
      }
    }
    return $rr;
  }

  protected function _isConditionOperand($key){
    $condition_operands = array('AND', 'OR');
    $_key = \trim($key);
    $_key = \strtoupper($_key);
    if(in_array($_key, $condition_operands)){
      return true;
    }else{
      return false;
    }
  }

  protected function _glue($key, $values){
    if(\is_array($values)){
      if(count($values)>1){
        $condition_operands = $this->_where_operands;
        $_key = \trim($key);
        $_key = \strtoupper($_key);
//          print_r(array($_key, $values)); exit;
//          print_r($condition_operands); exit;
        if(in_array($_key, $condition_operands)){
          $glue = ' ' . $_key . ' ';
          return \implode($glue, $values);
        }else{
          throw new \Exception('Invalid condition operand in WHERE clause.');
          return '';
        }
      }else{
        if(count($values)==1){
          return ' ' . \current($values);
        }else{
          throw new \Exception('ERROR. Empty $values in _glue [DAL].');
        }
      }

    }else{
      //print_r(array($key, $values)); exit;
      throw new \Exception('NOT_IMPLEMENTED _glue().');
    }

  }

  protected function _predicate($k, $v){
    if($v === '__CUSTOM_SQL__'){
      return $k;
    }
    //print_r(array($k, $v)); exit;
    $predicate = false;
    $operands = $this->_where_operands;
    $predicates = $this->_where_predicates;
    if(\is_numeric($k)){
      // case with no params, $v is predicate
      if(\is_string($v)){
        return $v;
      }else{
        throw new \Exception('Invalid predicate in WHERE clause: ' . print_r($v, true));
        return '';
      }
    }

//    $_k = $k; $_v = $v;
//    $k = 'b = ? AND (b <> ? or b>?)'; $v = array(2,3,4); // multiple params // Complex predicates NOT implemented
//    $k = 'b = ? AND b <> ?'; $v = array(2,3); // multiple params // Complex predicates NOT implemented
//    $k = 'c = ?'; $v = array(3); //
//    $k = 'c = '; $v = 3; //
//    $k = 'c = ?a'; $v = 3; //
//    $k = 'c = ?'; $v = 3; //
//    $k = $_k; $v = $_v;
//    $k = 'c ='; $v = 3; //
//    $k = 'c'; $v = 3; // no predicate, default is '='
    if(\is_string($k)){

      // look for predicate operands [case of multiple params]
      foreach($operands as $op){
        $op = ' '.$op.' ';
        if(\stripos($k, $op)){
          //throw new \Exception("Error! Complex predicates NOT implemented. [$op in $k]");
          return $k;
        }
        //$k = \str_ireplace($op, $op, $k);
      }

      //array('NOT IN', 'IN', 'NOT BETWEEN' 'BETWEEN','NOT LIKE', 'LIKE', 'IS NULL', 'IS NOT NULL')
      foreach($predicates['complex'] as $pr){
        $pr_pos = \stripos($k, ' '.$pr.' ');
        if($pr_pos){
          $subject = \trim(\substr($k, 0, $pr_pos));
          $subject_info = $this->_extractSubjectInfo($subject);
          if($subject_info['alias'] != $this->_getRootAlias()){
            $this->_use_limit_subquery_in_from = false;
          }

          $predicate = $pr;
          $object = \trim(\substr($k, $pr_pos + \strlen(' '.$pr.' ')));

          //\hat\dbg::alert(array($subject, $subject_info, $predicate, $object),true);
          if($object != '?' && !\in_array($predicate, array('NOT LIKE', 'LIKE', 'IS NULL', 'IS NOT NULL'))){
            //\hat\dbg::alert(array($subject, $subject_info, $predicate, $object),true);
            throw new \Exception("Missing '?' in predicate '$k'.");
          }


          if($predicate == 'IN' || $predicate == 'NOT IN'){
            if($this->_query_debug){
              //\hat\dbg::alert(compact('k', 'v'));
            }
            if(\is_array($v)){
              $param_count = \count($v);
              if($param_count == 0){
                //throw new \Exception("Empty array for IN predicate.");
                \trigger_error("Empty array for IN predicate [$k].", \E_USER_WARNING);
                return '1=0';
              }
              $_in_ = '';
              for($i=0; $i<$param_count; $i++){
                $this->_BIND_WHERE_PARAMS[] = array('type' => $subject_info['subject_type'], 'value' => $v[$i]);
                if($this->_query_debug){
                  $_v = $v[$i];
                }else{
                  $_v = '?';
                }

                if(empty($_in_)){
                  $_in_ .= $_v;
                }else{
                  $_in_ .= ', ' . $_v;
                }
              }

            }else{
              throw new \Exception("Param for $predicate predicate must by array.");
            }

            return "{$subject_info['full_subject']} $predicate ( $_in_ )";
          }

          if($predicate == 'BETWEEN' || $predicate == 'NOT BETWEEN'){
            if(\is_array($v)){
              $param_count = \count($v);
              if($param_count != 2){
                throw new \Exception("Wrong number of params in $k.");
                //\trigger_error("Empty array for IN predicate [$k].", \E_USER_WARNING);
                return '1=0';
              }
              $_in_ = '';
              $this->_BIND_WHERE_PARAMS[] = array('type' => $subject_info['subject_type'], 'value' => $v[0]);
              $this->_BIND_WHERE_PARAMS[] = array('type' => $subject_info['subject_type'], 'value' => $v[1]);
              if($this->_query_debug){
                $_v_1 = $v[0];
                $_v_2 = $v[1];
              }else{
                $_v_1 = '?';
                $_v_2 = '?';
              }

              $_in_ .= $_v_1 . ' AND ' . $_v_2;

            }else{
              throw new \Exception("Param for $predicate predicate must by array.");
            }

            return "{$subject_info['full_subject']} $predicate $_in_ ";
          }

          if($predicate == 'LIKE' || $predicate == 'NOT LIKE'){
              $_in_ = '';
              $this->_BIND_WHERE_PARAMS[] = array('type' => $subject_info['subject_type'], 'value' => $v);
              if($this->_query_debug){
                $_v_1 = $v;
              }else{
                $_v_1 = '?';
              }

              $_in_ .= "($_v_1)";

            return "{$subject_info['full_subject']} $predicate $_in_ ";
          }

          if($predicate == 'IS NULL' || $predicate == 'IS NOT NULL'){
            return "{$subject_info['full_subject']} $predicate ";
          }

          throw new \Exception("Complex predicates NOT implemented ($k).");
        }
      }

      foreach($predicates['simple'] as $pr){
        $pr_pos = \strpos($k, $pr);
        if($pr_pos){
          $subject = \trim(\substr($k, 0, $pr_pos));
          $subject_info = $this->_extractSubjectInfo($subject);
          $predicate = $pr;
          $object = \trim(\substr($k, $pr_pos + \strlen($predicate)));
//          \hat\dbg::alert(array($subject, $predicate, $object, $v));
          if($object != '?'){
            return "{$subject_info['full_subject']} $predicate $object";
            //throw new \Exception("Error! Missing '?' in predicate '$k'.");
          }

          if(\is_array($v) || \is_object($v)){
            throw new \Exception("Error! Invalid parameter for predicate '$k'.");
          }

          //return "$subject $predicate $v";

          //\hat\dbg::alert(array($subject, $subject_info, $predicate, $object, $v));
          if($subject_info['alias'] != $this->_getRootAlias()){
            $this->_use_limit_subquery_in_from = false;
          }

          $this->_BIND_WHERE_PARAMS[] = array('type' => $subject_info['subject_type'], 'value' => $v);

          if($this->_query_debug){
            return "{$subject_info['full_subject']} $predicate $v";
          }
          return "{$subject_info['full_subject']} $predicate $object";

        }
      }

      throw new \Exception("Error! Predicate not parsed in $k clause. #1736");

    }


    throw new \Exception("Error! Predicate not parsed in WHERE clause, predicate should be string.");
  }

  protected function _extractSubjectInfo($subject){
    $_alias = false;
    $full_subject = $subject;
    $dot = \strrpos($subject, '.');
    if($dot !== false){
      $_alias = \substr($subject, 0, $dot);
      $_alias = trim($_alias, " ()");
      $subject = \substr($subject, $dot + 1);
    }else{
      $_alias = $this->_root_table['model_alias'];
      //$full_subject = $_alias .'.'. $subject;
    }

    $table_name = $this->_getTableNameByAlias($_alias);
    if(isset($this->_tables[$table_name])){
      $type = $this->_tables[$table_name]->columnInfo($subject);
      if($type){
        ;
      }else{
        throw new \Exception("Error! Column $subject not found in $table_name.");
      }
    }else{
      //\hat\dbg::alert($_alias);
      //\hat\dbg::alert($table_name);
      //\hat\dbg::alert($this->_tables_in_use);
      //\hat\dbg::alert(array_keys($this->_tables));
      throw new \Exception("Error! Table for alias $_alias not found in this query.");
    }

    $info = array(
        'alias' => $_alias,
        'subject' => $subject,
        'full_subject' => $full_subject,
        'subject_type' => $type,
    );
    return $info;
  }

  final public function groupBy(){

    return $this;
  }

  final public function having(){

    return $this;
  }

  final public function addOrderBy($order){
    $this->_query_parts['order_by'][] = \trim($order);
    return $this;
  }
  final public function addOrderByAsSql($order){
    $this->_query_parts['order_by'][] = array('sql' => $order);
    return $this;
  }
  final public function orderBy($order){
    $order_parts = \explode(',', $order);
    foreach($order_parts as $order_part){
      $this->_query_parts['order_by'][] = \trim($order_part);
    }
    return $this;
  }

  final public function limit($limit){
    $this->_query_parts['limit'][] = $limit;
    return $this;
  }

  final public function offset($offset){
    $this->_query_parts['offset'][] = $offset;
    return $this;
  }

  final public function free(){
    $this->reset();
  }

  final public function reset($all = true){
    $this->_sql = '';

    if($all){
      $this->_query_parts = $this->_empty_query_parts;
    }
    $this->_sql_query_parts = $this->_empty_query_parts;

    $this->_UPDATE = array();
    $this->_SET = array();
    $this->_INSERT = array();
    $this->_VALUE = array();
    $this->_DELETE = array();
    $this->_SELECT = array();
    $this->_RETURNING = array();
    $this->_FROM = array();
    $this->_WHERE = array();
    $this->_JOIN = array();
    $this->_GROUP_BY = array();
    $this->_HAVING = array();
    $this->_ORDER_BY = array();
    $this->_LIMIT = array();
    $this->_OFFSET = array();

    $this->_BIND_PARAMS = array();
    $this->_BIND_WHERE_PARAMS = array();
    $this->results = array();

    $this->_query_debug = false;
    $this->_its_count_query = false;
    return $this;
  }

  // query preparers:

    // update
  protected function _prepareQueryParts_update(){
    //print_r($this->_query_parts);
    foreach($this->_query_parts['update'] as $k => $update){
      $update_parts = \explode(',', $update);
      foreach($update_parts as $update_part){
        $update_part = \trim($update_part);
        $has_alias = \strripos($update_part, ' as ');
        if($has_alias !== false){
          $model_name = \substr($update_part, 0, $has_alias);
          $model_alias = \substr($update_part, $has_alias+4);
        }else{
          $has_alias = \strrpos($update_part, ' ');
          if($has_alias !== false){
            $model_name = \substr($update_part, 0, $has_alias);
            $model_alias = \substr($update_part, $has_alias+1);
          }else{
            $model_name = $model_alias = $update_part;
          }
        }


        $model = \hatwebtech\dal\DAL::load($model_name);
        $this->_tables[$model_name] = $model;
        $primary_keys = $this->_tables[$model_name]->getPrimaryKeys();
        $table_info = array('model_name' => $model_name, 'model_alias' => $model_alias, 'primary_keys' => $primary_keys);
        $this->_tables_in_use[] = $table_info;
        $this->_UPDATE[] = $table_info;
        if(!isset($this->_root_table)){
          $this->_root_table = $table_info;
        }
      }
    }

    //print_r($this->_UPDATE); exit;
    //print_r($this->_query_parts); exit;

    return true;
  }
    // set
  protected function _prepareQueryParts_set(){
    //print_r($this->_query_parts); exit;
    $this->_SET = $this->_query_parts['set'];

    return true;
  }
    // insert
  protected function _prepareQueryParts_insert(){
    //print_r($this->_query_parts); exit;
    foreach($this->_query_parts['insert'] as $k => $insert){
      $insert_parts = \explode(',', $insert);
      foreach($insert_parts as $insert_part){
        $insert_part = \trim($insert_part);
        $has_alias = \strripos($insert_part, ' as ');
        if($has_alias !== false){
          $model_name = \substr($insert_part, 0, $has_alias);
          $model_alias = \substr($insert_part, $has_alias+4);
        }else{
          $has_alias = \strrpos($insert_part, ' ');
          if($has_alias !== false){
            $model_name = \substr($insert_part, 0, $has_alias);
            $model_alias = \substr($insert_part, $has_alias+1);
          }else{
            $model_name = $model_alias = $insert_part;
          }
        }


        $model = \hatwebtech\dal\DAL::load($model_name);
        $this->_tables[$model_name] = $model;
        $primary_keys = $this->_tables[$model_name]->getPrimaryKeys();
        $table_info = array('model_name' => $model_name, 'model_alias' => $model_alias, 'primary_keys' => $primary_keys);
        $this->_tables_in_use[] = $table_info;
        $this->_INSERT[] = $table_info;
        if(!isset($this->_root_table)){
          $this->_root_table = $table_info;
        }
      }
    }

    //print_r($this->_INSERT); exit;
    //print_r($this->_query_parts); exit;


    return true;
  }
    // value
  protected function _prepareQueryParts_value(){
    //print_r($this->_query_parts); exit;
    $this->_VALUE = $this->_query_parts['value'];
    //print_r($this->_VALUE); exit;

    return true;
  }
    // values
  protected function _prepareQueryParts_values(){
    //print_r($this->_query_parts); exit;
    $this->_VALUES = $this->_query_parts['values'];
    //print_r($this->_VALUES); exit;

    return true;
  }
    // insert fields
  protected function _prepareQueryParts_fields(){
    //print_r($this->_query_parts); exit;
    $this->_FIELDS = $this->_query_parts['fields'];
    //print_r($this->_FIELDS); exit;

    return true;
  }
    // delete
  protected function _prepareQueryParts_delete(){
    //print_r($this->_query_parts); exit;
    foreach($this->_query_parts['delete'] as $k => $delete){
      $delete_parts = \explode(',', $delete);
      foreach($delete_parts as $delete_part){
        $delete_part = \trim($delete_part);
        $has_alias = \strripos($delete_part, ' as ');
        if($has_alias !== false){
          $model_name = \substr($delete_part, 0, $has_alias);
          $model_alias = \substr($delete_part, $has_alias+4);
        }else{
          $has_alias = \strrpos($delete_part, ' ');
          if($has_alias !== false){
            $model_name = \substr($delete_part, 0, $has_alias);
            $model_alias = \substr($delete_part, $has_alias+1);
          }else{
            $model_name = $model_alias = $delete_part;
          }
        }


        $model = \hatwebtech\dal\DAL::load($model_name);
        $this->_tables[$model_name] = $model;
        $table_info = array('model_name' => $model_name, 'model_alias' => $model_alias);
        $this->_tables_in_use[] = $table_info;
        $this->_DELETE[] = $table_info;
        if(!isset($this->_root_table)){
          $this->_root_table = $table_info;
        }
      }
    }

    //print_r($this->_DELETE); exit;
    //print_r($this->_query_parts); exit;

    return true;
  }
  protected function _prepareQueryParts_from(){

    $this->_FROM = array();
    foreach($this->_query_parts['from'] as $k => $from){
      $from_parts = \explode(',', $from);
      foreach($from_parts as $from_part){
        $from_part = \trim($from_part);
        $has_alias = \strripos($from_part, ' as ');
        if($has_alias !== false){
          $model_name = \substr($from_part, 0, $has_alias);
          $model_alias = \substr($from_part, $has_alias+4);
        }else{
          $has_alias = \strrpos($from_part, ' ');
          if($has_alias !== false){
            $model_name = \substr($from_part, 0, $has_alias);
            $model_alias = \substr($from_part, $has_alias+1);
          }else{
            $model_name = $model_alias = $from_part;
          }
        }


        $this->_tables[$model_name] = \hatwebtech\dal\DAL::load($model_name);
        $primary_keys = $this->_tables[$model_name]->getPrimaryKeys();
        foreach($primary_keys as $primary_key){
          $this->_SELECT[$model_alias][] = $primary_key;
        }
        $table_info = array('model_name' => $model_name, 'model_alias' => $model_alias, 'primary_keys' => $primary_keys);
        $this->_tables_in_use[] = $table_info;
        $this->_FROM[] = $table_info;
        if(!isset($this->_root_table)){
          $this->_root_table = $table_info;
        }

//          $filename = $this->_table_path . $model_name . '.php';
//          if(\file_exists($filename)){
//            include_once $filename;
//            $model_class_name = $this->_table_namespace . $model_name;
//            $this->_tables[$model_name] = new $model_class_name();
//            $table_info = array('model_name' => $model_name, 'model_alias' => $model_alias);
//            $this->_tables_in_use[] = $table_info;
//            $this->_FROM[] = $table_info;
//          }else{
//            throw new \Exception("Module not found for $model_name in $this->_table_path.");
//          }

      }
    }

    //print_r($this->_FROM); exit;
    //print_r($this->_query_parts); exit;
    return true;
  }

  protected function _prepareQueryParts_join(){
    // joins
    $this->_additional_select = array();

    foreach($this->_query_parts['join'] as $i => $join){
      if($join['type'] == 'left'){
        // example: hi.HatTemplate ht
        $dot = \strpos($join['join'], '.');
        if($dot !== false){
          $left_alias = \substr($join['join'], 0, $dot);
          $right_part = \substr($join['join'], $dot + 1);

          $left_table_name = $this->_getTableNameByAlias($left_alias);
          if(!$left_table_name){
            $this->pdo_error_message[] = "Table object for alias $left_alias not found in this query.";
            return false;
          }

          //
          $has_alias = \strripos($right_part, ' as ');
          if($has_alias !== false){
            $assoc_name = \substr($right_part, 0, $has_alias);
            $assoc_alias = \substr($right_part, $has_alias+4);
          }else{
            $has_alias = \strrpos($right_part, ' ');
            if($has_alias !== false){
              $assoc_name = \substr($right_part, 0, $has_alias);
              $assoc_alias = \substr($right_part, $has_alias+1);
            }else{
              $assoc_name = $assoc_alias = $right_part;
            }
          }

          $associ_info = $this->_tables[$left_table_name]->associationInfo($assoc_name);
          if(!$associ_info){
            $this->pdo_error_message[] = "Association $assoc_name not found in $left_table_name.";
            return false;
          }

          $model_name = $associ_info['association_name'];

          $this->_tables[$model_name] = \hatwebtech\dal\DAL::load($model_name);
          $primary_keys = $this->_tables[$model_name]->getPrimaryKeys();
          $table_info = array(
              'model_name' => $model_name,
              'model_alias' => $assoc_alias,
              'primary_keys' => $primary_keys,
              'assoc_name' => $assoc_name,
              'assoc_to_alias' => $left_alias,
              'assoc_to_name' => $left_table_name,
              'assoc_info' => $associ_info,
              'join_index' => $i
            );
            //\hat\dbg::alert($right_part);
          if($right_part == 'HatSystemConfigArray ca'){
            //\hat\dbg::alert($table_info);
            //\hat\dbg::alert($this->_tables[$left_table_name]->toArray(), true);
          }
          $this->_tables_in_use[] = $table_info;
          if($associ_info['type'] == 'hasMany'){
            //$this->_has_one_to_many_assoc = true;
            $this->_has_one_to_many_assoc = $associ_info;
          }
          $this->_additional_select[$left_alias][] = $associ_info['keys']['local'];
          $this->_additional_select[$assoc_alias][] = $associ_info['keys']['foreign'];
          $this->_additional_select[$assoc_alias] = \array_merge($this->_additional_select[$assoc_alias], $this->_tables[$model_name]->getPrimaryKeys());

          $this->_join_pairs[$left_alias . '__' . $associ_info['keys']['local']] = $assoc_alias . '__' . $associ_info['keys']['foreign'];
          $this->_join_node_keys['local'][$left_alias . '__' . $associ_info['keys']['local']] = true;
          $this->_join_node_keys['foreign'][$assoc_alias . '__' . $associ_info['keys']['foreign']] = true;

          $assoc_table_name = $this->_tables[$model_name]->getTableName();
          $join_sql = " LEFT JOIN $assoc_table_name AS $assoc_alias ON $left_alias.{$associ_info['keys']['local']}=$assoc_alias.{$associ_info['keys']['foreign']} ";
          if(isset($join['condition'])){
            //\hat\dbg::alert($join['condition'], true);
            //\hat\dbg::alert(array($i => $join));
            //\hat\Object::__d($join);
            $prepare = $this->_prepareQueryParts_where($join['condition']);
            //\hat\dbg::alert($prepare, true);
            if($prepare){
              $parse = $this->_parseQueryParts_where($prepare);
              if($this->_query_debug){
                //\hat\dbg::alert($parse);
              }
              if($parse && isset($parse[0])){
                $join_sql .= " AND {$parse[0]} ";
//                \hat\dbg::alert($parse);
//                \hat\dbg::alert($join_sql, true);
              }
            }
          }
          $this->_JOIN[$assoc_alias] = $join_sql;

//            $filename = $this->_table_path . $model_name . '.php';
//            if(\file_exists($filename)){
//              include_once $filename;
//              $model_class_name = $this->_table_namespace . $model_name;
//              $this->_tables[$model_name] = new $model_class_name();
//              $table_info = array(
//                  'model_name' => $model_name,
//                  'model_alias' => $assoc_alias,
//                  'assoc_name' => $assoc_name,
//                  'assoc_to_alias' => $left_alias,
//                  'assoc_to_name' => $left_table_name,
//                  'assoc_info' => $associ_info
//                );
//              $this->_tables_in_use[] = $table_info;
//              if($associ_info['type'] == 'hasMany'){
//                //$this->_has_one_to_many_assoc = true;
//                $this->_has_one_to_many_assoc = $associ_info;
//              }
//              $this->_additional_select[$left_alias][] = $associ_info['keys']['local'];
//              $this->_additional_select[$assoc_alias][] = $associ_info['keys']['foreign'];
//
//              $this->_join_pairs[$left_alias . '__' . $associ_info['keys']['local']] = $assoc_alias . '__' . $associ_info['keys']['foreign'];
//              $this->_join_node_keys['local'][$left_alias . '__' . $associ_info['keys']['local']] = true;
//              $this->_join_node_keys['foreign'][$assoc_alias . '__' . $associ_info['keys']['foreign']] = true;
//
//              $assoc_table_name = $this->_tables[$model_name]->getTableName();
//              $join_slq = " LEFT JOIN $assoc_table_name AS $assoc_alias ON $left_alias.{$associ_info['keys']['local']}=$assoc_alias.{$associ_info['keys']['foreign']} ";
//              $this->_JOIN[$assoc_alias] = $join_slq;
//
//            }else{
//              throw new \Exception("Module not found for $model_name in $this->_table_path.");
//            }


        }else{
          $this->pdo_error_message[] = 'invalid join sintax.';
          return false;
        }

      }// end left join

    }
//    print_r($this->_JOIN); exit;
//    print_r($this->_additional_select);

    return true;
  }

  protected function _prepareQueryParts_returning(){
    $this->_RETURNING = $this->_query_parts['returning'];

    return true;
  }

  protected function _prepareQueryParts_select(){
    // select
    foreach($this->_query_parts['select'] as $k => $select){
      $select_parts = \explode(',', $select);
//      print_r(array($select, $select_parts),true);
      foreach($select_parts as $select_part){
        $select_part = \trim($select_part);
        $dot = \strrpos($select_part, '.');
        if($dot !== false){
          $_alias = \substr($select_part, 0, $dot);
          $select_part = \substr($select_part, $dot + 1);
        }else{
        //if(in_array($select_part, \array_keys($this->_tableColumns))){
          $_alias = $this->_root_table['model_alias'];
        }
        if($select_part == '*'){
          if(!isset($this->_SELECT[$_alias])){
            $this->_SELECT[$_alias] = array();
          }
//    print_r($_alias);
//    print_r($this->_getTableNameByAlias($_alias)); exit;
          $columns = $this->_tables[$this->_getTableNameByAlias($_alias)]->getColumns();
          $this->_SELECT[$_alias] = \array_merge($this->_SELECT[$_alias], $columns);
        }else{
          $this->_SELECT[$_alias][] = $select_part;
        }
      }
    }
//    print_r($this->_SELECT); exit;
//    print_r($this->_query_parts['where']); exit;

    return true;
  }

  protected function _prepareQueryParts_where($return = false){
    if($return){
      $__where = $return;
    }else{
      $__where = $this->_query_parts['where'];
    }
    if(empty($__where)){
      $where = array();
    }else{
      $where = $__where;
      if(!\is_array($where)){
        $where = array($where);
      }
      if(count($where)>1){
        $has_op = false;
        $op = \strtoupper(\current($where));
        foreach($this->_where_operands as $_op){
          if($op == $_op){
            $has_op = true;
          }
        }
        if(!$has_op){
          $where = array('AND' => $where);
        }
      }

    }
    if($return){
      return $where;
    }
    $this->_WHERE = $where;

//    print_r($this->_WHERE);
    //print_r($this->_WHERE); exit;

    return true;
  }

  protected function _prepareQueryParts_order_by(){
    if(empty($this->_query_parts['order_by'])){
      $_queryPart = array();
    }else{
      $_queryPart = array();
      $queryParts = $this->_query_parts['order_by'];
      if(!\is_array($queryParts)){
        $queryParts = array($queryParts);
      }
//      //\hat\dbg::elog(print_r($queryParts, true));
      foreach($queryParts as $k => $queryPart){

        if(is_array($queryPart)){
          if(isset($queryPart['sql'])){
            //$_queryPart[] = array('as' => 'sql', 'sql' => $queryPart);
            $_queryPart[] = array('as' => 'sql', 'sql' => $queryPart['sql']);
//            \hat\dbg::elog('order_by as sql');
          }else{
//            \hat\dbg::elog('ERROR in order_by!!!');
          }
          continue;
        }else{
//          \hat\dbg::elog("order_by = $queryPart");
        }

        // if($k == 'sql') IS ALLWAYS TRUE !!!!!!!!!!!!!!!!!!!!!!!
//        if($k == 'sql'){
//          $_queryPart[] = array('as' => 'sql', 'sql' => $queryPart);
//          \hat\dbg::elog(print_r(array('as' => 'sql', 'sql' => $queryPart, 'k' => $k), true));
//          continue;
//        }else{
//          \hat\dbg::elog(print_r($queryPart, true));
//        }
        $parts = \explode(' ', $queryPart);
        foreach($parts as $k => $part){
          if(empty($part)){
            unset($parts[$k]);
          }
        }
        // reset array keys after unset
        $parts = array_values($parts);

        $subject = $parts[0];

        $dot = \strrpos($subject, '.');
        if($dot !== false){
          $_alias = \substr($subject, 0, $dot);
          $_field = \substr($subject, $dot + 1);
        }else{
          $_alias = $this->_getRootAlias();
          $_field = $subject;
        }
        $this->_additional_select[$_alias][] = $_field;

        if($_alias != $this->_getRootAlias()){
          $this->_use_orderby_subquery_in_from = false;
        }

        if(isset($parts[1])){
          $direction = \strtoupper(\trim($parts[1]));
        }else{
          $direction = 'ASC';
        }

        $collate = '';
        if(! \in_array($direction, array('ASC', 'DESC'))){
          // it's not direction
          // is it COLLATE?
          if($direction == 'COLLATE' && isset($parts[2])){
            $collate = ' COLLATE ' . $parts[2];
            if(isset($parts[3])){
              $direction = \strtoupper(\trim($parts[3]));
            }else{
              $direction = 'ASC';
            }
            if(! \in_array($direction, array('ASC', 'DESC'))){
              $direction = 'ASC';
            }
          }else{
            //$direction = 'ASC';
//            \hat\dbg::alert($direction);
//            \hat\dbg::alert($parts);
          }
        }

        $_queryPart[] = \compact('_alias', '_field', 'collate', 'direction');
//        \hat\dbg::alert($parts);
//        \hat\dbg::alert($_queryPart);
      }
    }
    $this->_ORDER_BY = $_queryPart;

    //print_r($this->_ORDER_BY); exit;

    return true;
  }

  protected function _prepareQueryParts_limit(){
    if(empty($this->_query_parts['limit'])){
      $queryPart = array();
    }else{
      $queryPart = $this->_query_parts['limit'];
      if(!\is_array($queryPart)){
        $queryPart = array($queryPart);
      }
    }
    $this->_LIMIT = $queryPart;
    //print_r($this->_LIMIT); exit;

    return true;
  }

  protected function _prepareQueryParts_offset(){
    if(empty($this->_query_parts['offset'])){
      $queryPart = array();
    }else{
      $queryPart = $this->_query_parts['offset'];
      if(!\is_array($queryPart)){
        $queryPart = array($queryPart);
      }
    }
    $this->_OFFSET = $queryPart;
    //print_r($this->_OFFSET); exit;

    return true;
  }

  protected function _prepareQueryParts(){
    //\hat\dbg::timmer('prepare start');
    if($this->_prepareQueryParts_from() === false){return false;}
    if($this->_prepareQueryParts_join() === false){return false;}
    // if query_parts['select'] not empty
    $do_repeat = false;
    if(!empty($this->_query_parts['join'])){
      //\hat\dbg::timmer('prepare start [join]');
//      \hat\dbg::alert($this->_query_parts['join']);
//      \hat\dbg::alert($this->_tables_in_use);
      //\hat\dbg::alert('kraj', true);
      foreach($this->_tables_in_use as $_table){
        if(isset($_table['join_index'])){
          $table = $this->_tables[$_table['model_name']];
          $table->setAlias($_table['model_alias']);
          //\hat\dbg::alert($_table['model_alias'] . ' :: behave()');
          $r = $table->behave('DalJoin', $this);
          //\hat\dbg::alert($_table['model_alias'] . ' :: behave() = ' . $r);
          $do_repeat = $do_repeat || $r;
        }
      }
      if($do_repeat){
        if($this->_prepareQueryParts_join() === false){return false;}
        //\hat\dbg::timmer('prepare repeat');
      }
      //\hat\dbg::timmer('prepare end [select]');
    }

    if($this->_prepareQueryParts_update() === false){return false;}
    if($this->_prepareQueryParts_set() === false){return false;}
    $do_repeat = false;
    if(!empty($this->_query_parts['update'])){
      //\hat\dbg::alert($this->_query_parts, true);
      //\hat\dbg::timmer('prepare start [update]');
      foreach($this->_query_parts['update'] as $_table_name){
        $table = $this->_tables[$_table_name];
        $table->setAlias($_table_name);
        //\hat\dbg::alert($_table['model_alias'] . ' :: behave()');
        $r = $table->behave('DalUpdate', $this);
//        $do_repeat = $do_repeat || $r;
      }
//      if($do_repeat){
//        if($this->_prepareQueryParts_select() === false){return false;}
//        \hat\dbg::timmer('prepare repeat');
//      }
//      \hat\dbg::timmer('prepare end [update]');
    }

    if($this->_prepareQueryParts_insert() === false){return false;}
    if($this->_prepareQueryParts_value() === false){return false;}
    $do_repeat = false;
    // TODO make better condition check
    if(!empty($this->_query_parts['insert']) && empty($this->_query_parts['fields'])){
      //\hat\dbg::timmer('prepare start [insert]');
      //\hat\dbg::alert($this->_query_parts, true);
      //\hat\dbg::alert('delete', true);
      foreach($this->_query_parts['insert'] as $_table_name){
        $table = $this->_tables[$_table_name];
        $table->setAlias($_table_name);
        //\hat\dbg::alert($_table['model_alias'] . ' :: behave()');
        $r = $table->behave('DalInsert', $this);
        $do_repeat = $do_repeat || $r;
      }
      //\hat\dbg::timmer('prepare end [insert]');
      if($do_repeat){
        if($this->_prepareQueryParts_value() === false){return false;}
        //\hat\dbg::timmer('prepare repeat');
      }
    }
    if($this->_prepareQueryParts_fields() === false){return false;}
    if($this->_prepareQueryParts_values() === false){return false;}

    if($this->_prepareQueryParts_delete() === false){return false;}
    $do_repeat = false;
    if(!empty($this->_query_parts['delete'])){
      //\hat\dbg::timmer('prepare start [delete]');
      //\hat\dbg::alert($this->_query_parts, true);
      //\hat\dbg::alert('delete', true);
      foreach($this->_query_parts['delete'] as $_table_name){
        $table = $this->_tables[$_table_name];
        $table->setAlias($_table_name);
        //\hat\dbg::alert($_table['model_alias'] . ' :: behave()');
        $r = $table->behave('DalDelete', $this);
      }
      //\hat\dbg::timmer('prepare end [delete]');
    }

    if($this->_prepareQueryParts_select() === false){return false;}
    // if query_parts['select'] not empty
    $do_repeat = false;
    if(!empty($this->_query_parts['select'])){
      //\hat\dbg::timmer('prepare start [select]');
      foreach($this->_tables_in_use as $_table){
        if(!isset($_table['join_index'])){
          $table = $this->_tables[$_table['model_name']];
          $table->setAlias($_table['model_alias']);
          //\hat\dbg::alert($_table['model_alias'] . ' :: behave()');
          $r = $table->behave('DalSelect', $this);
          $do_repeat = $do_repeat || $r;
        }
      }
      if($do_repeat){
        if($this->_prepareQueryParts_select() === false){return false;}
        //\hat\dbg::timmer('prepare repeat');
      }
      //\hat\dbg::timmer('prepare end [select]');
    }

    if($this->_prepareQueryParts_where() === false){return false;}
    if($this->_prepareQueryParts_returning() === false){return false;}
    if($this->_prepareQueryParts_order_by() === false){return false;}
    if($this->_prepareQueryParts_limit() === false){return false;}
    if($this->_prepareQueryParts_offset() === false){return false;}

//    print_r($this->_additional_select);
//    print_r($this->_SELECT);
    foreach($this->_additional_select as $_additional_alias => $_additional_columns){
      if(!isset($this->_SELECT[$_additional_alias])){
        $this->_SELECT[$_additional_alias] = array();
      }
      foreach($_additional_columns as $_additional_column){
        if(!in_array($_additional_column, $this->_SELECT[$_additional_alias])){
          $this->_SELECT[$_additional_alias][] = $_additional_column;
        }
      }
    }

    return true;
  }

  // query parsers:

  protected function _parseQueryParts_select(){
    //select
    if($this->_its_count_query){
      $f = $this->_getRootTableName();
      $s_alias = $this->_getRootAlias();
      $primary_key = $this->_getRootTable()->getPrimaryKeys();
      if(\count($primary_key) == 1){
        $primary_key = $primary_key[0];
      }else{
        throw new \Exception("No primary key in $f or using composite keys.");
      }
      $this->_sql_query_parts['select'] = array();
      $this->_sql_query_parts['select'][] = "COUNT ( DISTINCT $s_alias.$primary_key) AS count_query ";
    }else{
      foreach($this->_SELECT as $alias => $select_parts){
        //print_r($select_part);
        foreach($select_parts as $select_part){
          $s = $alias . '.' . $select_part . ' AS ' . $alias . '__' . $select_part;
          $this->_sql_query_parts['select'][] = $s;
        }
      }
    }
//        print_r('end'); exit;

    return true;
  }

  protected function _parseQueryParts_returning(){
    //returning
    $this->_sql_query_parts['returning'] = $this->_query_parts['returning'];
    return true;
  }

    // update
  protected function _parseQueryParts_update(){

    //print_r($this->_UPDATE); exit;
    foreach($this->_UPDATE as $update_part){
      $u = $this->_tables[$update_part['model_name']]->getTableName();
      $this->_sql_query_parts['update'][] = $u;
    }
    //print_r($this->_sql_query_parts['update']); exit;
    return true;
  }
    // set
  protected function _parseQueryParts_set(){

    //\hat\dbg::alert($this->_SET);
//    \hat\dbg::alert('kraj', true);
    //print_r($this->_SET); exit;
    //print_r($this->_SET);
    foreach($this->_SET as $field => $value){
      // complex field ???
      // lft = lft + ?  =>  2
      $e_pos = \strpos($field, '=');
      $query_field = $field;
      if($e_pos){
        $_field = \trim(\substr($field, 0, $e_pos));
        //$_object = \trim(\substr($field, $e_pos+1));
        $field = $_field;
      }
      $type = $this->_getRootTable()->columnInfo($field);
      $this->_BIND_PARAMS[] = array('type' => $type, 'value' => $value);
      if($this->_query_debug){
        $_v = $value;
      }else{
        $_v = '?';
      }
      if($e_pos){
        $query_field = \str_replace('?', $_v, $query_field);
        $this->_sql_query_parts['set'][] = $query_field;
      }else{
        $this->_sql_query_parts['set'][] = $query_field . " = $_v";
      }
    }
    //\hat\dbg::alert($this->_sql_query_parts['set']);
    //print_r($this->_BIND_PARAMS); exit;
    return true;
  }
    // insert
  protected function _parseQueryParts_insert(){
    foreach($this->_INSERT as $insert_part){
      $u = $this->_tables[$insert_part['model_name']]->getTableName();
      $this->_sql_query_parts['insert'][] = $u;
    }
    //print_r($this->_sql_query_parts['insert']); exit;

    return true;
  }
    // value
  protected function _parseQueryParts_value(){
    //print_r($this->_VALUE); exit;
    //print_r($this->_VALUE);
    foreach($this->_VALUE as $field => $value){
      $type = $this->_getRootTable()->columnInfo($field);
      $this->_BIND_PARAMS[] = array('type' => $type, 'value' => $value);
      if($this->_query_debug){
        $_v = $value;
      }else{
        $_v = '?';
      }
      $this->_sql_query_parts['value']['fields'][] = $field;
      $this->_sql_query_parts['value']['values'][] = $_v;
    }
    //print_r($this->_BIND_PARAMS); exit;

    return true;
  }
    // value
  protected function _parseQueryParts_fields(){
    //print_r($this->_FIELDS); exit;
    //print_r($this->_FIELDS);
    $this->_sql_query_parts['fields'] = $this->_FIELDS;
//    foreach($this->_FIELDS as $field){
//      $this->_sql_query_parts['fields'][] = $field;
//    }

    return true;
  }
    // values
  protected function _parseQueryParts_values(){
    //print_r($this->_VALUES); exit;
    //print_r($this->_VALUES);
    $field_count = \count($this->_sql_query_parts['fields']);
    foreach($this->_VALUES as $values){
      if(\count($values) != $field_count){
        $this->pdo_error_message[] = "Values count don't match fields count [$field_count] in insert query.";
        return false;
      }
      $_values = array();
      $i = 0;
      foreach($values as $k => $value){
        $type = $this->_getRootTable()->columnInfo($this->_sql_query_parts['fields'][$i]);
        $this->_BIND_PARAMS[] = array('type' => $type, 'value' => $value);
        if($this->_query_debug){
          $_v = $value;
        }else{
          $_v = '?';
        }
        $_values[] = $_v;
        $i++;
      }
        $this->_sql_query_parts['values'][] = $_values;
    }
    //print_r($this->_BIND_PARAMS); exit;

    return true;
  }
    // delete
  protected function _parseQueryParts_delete(){
    foreach($this->_DELETE as $delete_part){
      $u = $this->_tables[$delete_part['model_name']]->getTableName();
      $this->_sql_query_parts['delete'][] = $u;
    }
    //print_r($this->_sql_query_parts['delete']); exit;

    return true;
  }
  protected function _parseQueryParts_from(){
    //from
    //print_r($this->_join_pairs);
    //print_r($this->_join_node_keys);
    //print_r($this->_has_one_to_many_assoc);
    //print_r('end'); exit;

    foreach($this->_FROM as $from_part){
      $f = $this->_tables[$from_part['model_name']]->getTableName();
      //if($from_part['model_name'] != $from_part['model_alias']){
        $f .= ' AS ' . $from_part['model_alias'];
      //}
      $this->_sql_query_parts['from'][] = $f;
    }

    return true;
  }
  protected function _parseSubQuerySqlParts(){
    foreach($this->_query_parts['sub_query_sql'] as $alias => $sql){
      $this->_sql_query_parts['from'][] = "($sql) AS $alias";
    }

    return true;
  }
  protected function _parseQueryParts_join(){
    //join
    //print_r($this->_JOIN); exit;
    foreach($this->_JOIN as $right_aliaas => $join){
      $this->_sql_query_parts['join'][] = $join;
    }

    return true;
  }
  protected function _parseQueryParts_where($return = false){
    //where
    if($return){
      $__where = $return;
      $__return_where = array();
      //\hat\dbg::alert($return);
    }else{
      $__where = $this->_WHERE;
    }
    if(!empty($__where)){
      try {
        //print_r($this->_WHERE ); exit;
        $_where = $this->_where($__where);
        $where = \end($_where);
        if($return){
          $__return_where[] = $where;
        }else{
          $this->_sql_query_parts['where'][] = $where;
        }
      } catch (\Exception $e) {
        throw $e;
        //print_r($e->getMessage());
        return false;
      }

    }
    if($return){
      return $__return_where;
    }

    return true;
  }
  protected function _parseQueryParts_order_by(){
    foreach($this->_ORDER_BY as $order_by){
      //if(isset($order_by['as']) && $order_by['as'] == 'sql' && isset($order_by['sql']) && isset($order_by['sql']['sql'])){
      if(isset($order_by['as']) && $order_by['as'] == 'sql' && isset($order_by['sql'])){
        //$this->_sql_query_parts['order_by'][] = $order_by['sql']['sql'];
        $this->_sql_query_parts['order_by'][] = $order_by['sql'];
//        \hat\dbg::elog(print_r($order_by['sql'], true));
      }else{
        $this->_sql_query_parts['order_by'][] = $order_by['_alias'] . '.' . $order_by['_field'] . $order_by['collate'] . ' ' . $order_by['direction'];
      }
      //$this->_sql_query_parts['order_by'][] = $order_by['_alias'] . '__' . $order_by['_field'] . $order_by['collate'] . ' ' . $order_by['direction'];
    }

//    print_r($this->_use_orderby_subquery_in_from);
//    print_r($this->_ORDER_BY);

    return true;
  }
  // offset & limit
  protected function _parseQueryParts_limit_offset(){
    $orderBy = $this->_additional_select = $limit = $offset = '';

    if(isset($this->_sql_query_parts['order_by'][0])){
      foreach($this->_ORDER_BY as $order_by){
        if(!empty($orderBy)){
          $orderBy .= ', ';
          //$this->_additional_select .= ', ';
        }
        //if(isset($order_by['as']) && $order_by['as'] == 'sql' && isset($order_by['sql']) && isset($order_by['sql']['sql'])){
        if(isset($order_by['as']) && $order_by['as'] == 'sql' && isset($order_by['sql'])){
          $orderBy .=  $order_by['sql'];
//        \hat\dbg::elog(print_r($order_by['sql'], true));
        }else{
          $orderBy .=  $order_by['_alias'] . '.' . $order_by['_field'] . ' ' . $order_by['direction'];
          $this->_additional_select .= ', ' . $order_by['_alias'] . '.' . $order_by['_field'];
        }
      }
      $orderBy = ' ORDER BY ' . $orderBy;
    }
    if(!empty($this->_LIMIT)){
      $limit = ' LIMIT ' . $this->_LIMIT[0];
    }
    if(!empty($this->_OFFSET)){
      $offset = ' OFFSET ' . $this->_OFFSET[0];
    }
    if($this->_has_one_to_many_assoc && (!empty($limit) || !empty($offset))){
      $f = $this->_getRootTableName();
      //$s_alias = $this->_root_table['model_alias'];
      $s_alias = $this->_getRootAlias();
      $primary_key = $this->_getRootTable()->getPrimaryKeys();
      if(\count($primary_key) == 1){
        $primary_key = $primary_key[0];
      }else{
        throw new \Exception("No primary key in $f or using composite keys.");
      }

      if($this->_use_limit_subquery_in_from && $this->_use_orderby_subquery_in_from){
        $form_subquery = '';
        $form_subquery .= ' (SELECT * FROM ' . $this->_getRootTableName() . ' AS ' . $this->_getRootAlias();
        if(isset($this->_sql_query_parts['where'][0]) && !empty($this->_sql_query_parts['where'][0])){
          $form_subquery .= ' WHERE 2=2 and ' . $this->_sql_query_parts['where'][0];
        }
        $form_subquery .= ' ' . $orderBy . $limit . $offset;
        $form_subquery .= ' ) AS ' . $this->_getRootAlias();

        $this->_sql_query_parts['from'][0] = $form_subquery;
        $this->_BIND_WHERE_PARAMS = \array_merge($this->_BIND_WHERE_PARAMS, $this->_BIND_WHERE_PARAMS);

      }else{
        $subquery = '';
        $subquery .= ' '.$s_alias.'.'.$primary_key." IN ( SELECT HAT_DAL_subquery.$primary_key FROM (";
        $subquery .= " SELECT DISTINCT $s_alias.$primary_key $this->_additional_select FROM $f AS $s_alias";
        foreach($this->_sql_query_parts['join'] as $j){
          $subquery .= $j;
        }
        $subquery .= ' WHERE 1=1 and  ' . $this->_sql_query_parts['where'][0];
//        \hat\dbg::alert($this->_BIND_WHERE_PARAMS);
        $this->_BIND_WHERE_PARAMS = \array_merge($this->_BIND_WHERE_PARAMS, $this->_BIND_WHERE_PARAMS);
//        \hat\dbg::alert($this->_BIND_WHERE_PARAMS);
        $subquery .= ' ' . $orderBy . $limit . $offset;
        $subquery .= ') AS HAT_DAL_subquery)';
//        \hat\dbg::alert($subquery);
//        \hat\dbg::alert($this->_sql_query_parts['where']);
        \array_unshift($this->_sql_query_parts['where'], $subquery);
//        \hat\dbg::alert($this->_sql_query_parts['where']);
      }
    }else{
      $this->_sql_query_parts['limit'][] = $limit;
      $this->_sql_query_parts['offset'][] = $offset;
    }

    return true;
  }

  protected function _parseQueryParts(){

    if($this->_parseQueryParts_select() === false){return false;}
    if($this->_parseQueryParts_update() === false){return false;}
    if($this->_parseQueryParts_set() === false){return false;}
    if($this->_parseQueryParts_insert() === false){return false;}
    if($this->_parseQueryParts_value() === false){return false;}
    if($this->_parseQueryParts_fields() === false){return false;}
    if($this->_parseQueryParts_values() === false){return false;}
    if($this->_parseQueryParts_delete() === false){return false;}
    if($this->_parseSubQuerySqlParts() === false){return false;}
    if($this->_parseQueryParts_from() === false){return false;}
    if($this->_parseQueryParts_join() === false){return false;}
    if($this->_parseQueryParts_where() === false){return false;}
    if($this->_parseQueryParts_returning() === false){return false;}
    if($this->_parseQueryParts_order_by() === false){return false;}
    if($this->_parseQueryParts_limit_offset() === false){return false;}

    // combine sql from sql_query_parts

    $this->_sql = '';

    if(isset($this->_sql_query_parts['select'][0])){
      $this->_sql .= 'SELECT ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['select']);
    }
    if(isset($this->_sql_query_parts['update'][0])){
      $this->_sql .= 'UPDATE ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['update']);
    }
    if(isset($this->_sql_query_parts['set'][0])){
      $this->_sql .= ' SET ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['set']);
    }
    if(isset($this->_sql_query_parts['insert'][0])){
      $this->_sql .= 'INSERT INTO ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['insert']);
    }
    if(isset($this->_sql_query_parts['value']['fields']) && isset($this->_sql_query_parts['value']['fields'][0])){
      $this->_sql .= ' ( ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['value']['fields']);
      $this->_sql .= ' ) VALUES ( ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['value']['values']);
      $this->_sql .= ' ) ';
    }

    // fields
    if(isset($this->_sql_query_parts['fields']) && isset($this->_sql_query_parts['values']) && isset($this->_sql_query_parts['values'][0])){
      $this->_sql .= ' ( ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['fields']);
      $this->_sql .= ' ) VALUES ';
    }
    // values
    if(isset($this->_sql_query_parts['values']) && isset($this->_sql_query_parts['values'][0])){
      $values = array();
      foreach($this->_sql_query_parts['values'] as $_values){
        $values[] = '(' . \implode(', ', $_values) . ')';
      }
      $this->_sql .= \implode(', ', $values);
    }

    if(isset($this->_sql_query_parts['delete'][0])){
      $this->_sql .= 'DELETE FROM ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['delete']);
    }
    if(isset($this->_sql_query_parts['from'][0])){
      $this->_sql .= ' FROM ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['from']);
    }
    foreach($this->_sql_query_parts['join'] as $sql_query_part){
      $this->_sql .= $sql_query_part;
    }
    if(isset($this->_sql_query_parts['where'][0])){
      $this->_sql .= ' WHERE ';
      $this->_sql .= \implode(' AND ', $this->_sql_query_parts['where']);
    }
    if(isset($this->_sql_query_parts['returning'][0])){
      $this->_sql .= ' RETURNING ';
      $this->_sql .= \implode(', ', $this->_sql_query_parts['returning']);
    }
    if(isset($this->_sql_query_parts['order_by'][0])){
      $this->_sql .= ' ORDER BY ' . \implode(', ', $this->_sql_query_parts['order_by']);
//      \hat\dbg::elog(print_r($this->_sql_query_parts['order_by'], true));
    }
    if(isset($this->_sql_query_parts['limit'][0])){
      $this->_sql .= $this->_sql_query_parts['limit'][0];
    }
    if(isset($this->_sql_query_parts['offset'][0])){
      $this->_sql .= $this->_sql_query_parts['offset'][0];
    }

    //print_r($this->_sql); exit;
    //print_r($this->_query_parts);
    //print_r($this->_use_limit_subquery_in_from);
    //print_r($this->_sql_query_parts);
    return true;
  }

  final public function haveErrors(){
    if(empty($this->pdo_error_message)) {
      return false;
    }
    return true;
  }
  final public function getLastPdoErrorMessage(){
    return end($this->pdo_error_message);
  }
  final public function getPdoErrorMessages(){
    return $this->pdo_error_message;
  }

  public function haveRootTable(){
    if(!isset($this->_root_table)){
      $this->_prepareQueryParts_from();
    }
    if(isset($this->_root_table)){
      return true;
    }
    return false;
  }

  public function getTableInfoByAlias($alias){
    foreach($this->_tables_in_use as $t){
      if($t['model_alias'] == $alias){
        return $t;
      }
    }
    return false;
  }
  public function getTableNameByAlias($alias){
    return $this->_getTableNameByAlias($alias);
  }
  protected function _getTableNameByAlias($alias){
    foreach($this->_tables_in_use as $t){
      if($t['model_alias'] == $alias){
        return $t['model_name'];
      }
    }
    return false;
  }

  protected function _getRootModelName(){
    if(!isset($this->_root_table)){
      throw new \Exception('Root table not set![1]');
    }
    return $this->_root_table['model_name'];
  }

  protected function _getRootTableName(){
    if(!isset($this->_root_table)){
      throw new \Exception('Root table not set![1]');
    }
    return $this->_tables[$this->_root_table['model_name']]->getTableName();
  }

  /**
   *
   * @return Table
   */
  public function getRootTable(){
    $this->_prepareQueryParts_from();
    return $this->_getRootTable();
  }
  /**
   *
   * @return Table
   */
  protected function _getRootTable(){
    if(!isset($this->_root_table)){
      throw new \Exception('Root table not set![2]');
    }
    return $this->_tables[$this->_root_table['model_name']];
  }

  public function getRootAlias(){
    $this->_prepareQueryParts_from();

//    \hat\dbg::alert($this->_tables, true);
    return $this->_getRootAlias();
  }
  protected function _getRootAlias(){
    if(!isset($this->_root_table)){
      //\hat\dbg::alert($this->_query_parts, true);
      throw new \Exception('Root table not set![3]');
    }
    return $this->_root_table['model_alias'];
  }

  final public function getTable($name){
    if(!isset($this->_tables[$name])){
      //\hat\dbg::alert(array_keys($this->_tables));
    }
    return $this->_tables[$name];
  }

  /**
   * NESTED SET METHODS:
   */

  /**
   *
   * @param int $id id of the tree node
   * @param string $table db table name where the tree is
   * @param mixed $options
   * @return mixed string or array representation of path or false on error
   * @todo use nestedset configuration? (for lft and rgt column names...)
   */
  public function getPath($id, $table, $options = array()){
    $get_info = true;
    $path_field = isset($options['path_field'])? $options['path_field']: 'name';
    $path_glue = isset($options['path_glue'])? $options['path_glue']: '/';

    if(isset($options['lft']) && isset($options['rgt'])){
      $lft = $options['lft'];
      $rgt = $options['rgt'];
      $get_info = false;
    }

    $r = array();
    if($get_info){
      $sql = "SELECT id, root_id, $path_field, lft, rgt, level FROM $table WHERE id = $id and deleted_at is null limit 1;";
      //print_r($sql); return;
      $r1 = $this->querySql($sql);
      if($r1 === false){
        $this->pdo_error_message[] = 'error getPath 1.';
        return false;
      }
      $r1 = $this->getResultsDbRow();
      if(isset($r1[0])){
        $lft = $r1[0]['lft'];
        $rgt = $r1[0]['rgt'];
        $root_id = $r1[0]['root_id'];
      }else{
        $this->pdo_error_message[] = 'error getPath 2.';
        return false;
      }
    }

    $sql = "SELECT h.$path_field FROM hat_file h WHERE (h.lft <= $lft AND h.rgt >= $rgt) AND h.root_id = $root_id AND (h.deleted_at IS NULL) ORDER BY h.lft asc";

//    echo $sql; exit;
    $r1 = $this->querySql($sql);
    if($r1 === false){
      $this->pdo_error_message[] = 'error getPath 3.';
      return false;
    }
    $r1 = $this->getResultsDbRow();
//    print_r($r1); exit;
    if(isset($r1[0])){
      $r = array();
      foreach($r1 as $_r){
        $r[] = $_r['name'];
      }
      if(isset($options['as_string']) && $options['as_string']){
        $s = \implode('/', $r);
        return \trim($s, '/');
      }
      return $r;
    }else{
      $this->pdo_error_message[] = 'error getPath 4.';
      return false;
    }

    return $r;

  }

  /**
   *
   * @param Table $table
   * @return bool
   */
  public function createRoot($table){
    if(!$table->isLoaded()){
      throw new \Exception('Node must be loaded.');
      return false;
    }
    if(!$table->isTree()){
      throw new \Exception('Model must be a tree.');
      return false;
    }
    if($table->isInTree()){
      throw new \Exception('Node already in a tree.');
      return false;
    }

    $site_id = \hat\common\SystemConfig::getSiteId();
    $id = $table->get('id');
    $table_name = $table->getTableName();

    $behavior_name = 'nestedset';
    $lft_key = $table->getBehavior()->getFieldName($behavior_name, 'lft');
    $rgt_key = $table->getBehavior()->getFieldName($behavior_name, 'rgt');
    $level_key = $table->getBehavior()->getFieldName($behavior_name, 'level');
    $root_id_key = $table->getBehavior()->getFieldName($behavior_name, 'root_id');


    $this->reset();
    $sql = "SELECT MAX($root_id_key) FROM $table_name WHERE $root_id_key > 0";
    if($table->getBehavior()->isIt('multitenant')){
      $tenant_identifier = $table->getBehavior()->getFieldName('multitenant', 'identifier');
      $sql .= " AND $tenant_identifier = $site_id ";
    }
    $r = $this->queryStmt($sql);
    if($r !== false){
      $r = $this->getResultsDbRow();
    }else{
      throw new \Exception("Error creating root:" . $this->getLastPdoErrorMessage());
      return false;
    }
    $root_id = 0;
    if(isset($r[0]) && isset($r[0]['max'])){
      $root_id = $r[0]['max'];
    }
    $root_id++;// increment for next root_id

    $this->reset();
    $sql = "UPDATE $table_name SET $root_id_key = $root_id, $lft_key = 1, $rgt_key = 2, $level_key = 0 WHERE id = $id ";
    if($table->getBehavior()->isIt('multitenant')){
      $tenant_identifier = $table->getBehavior()->getFieldName('multitenant', 'identifier');
      $sql .= " AND $tenant_identifier = $site_id ";
    }
    $r = $this->queryStmt($sql);
    if($r !== false){
      $r = $this->getResultsDbRow();
    }else{
      throw new \Exception("Error creating root:" . $this->getLastPdoErrorMessage());
      return false;
    }
    //\hat\dbg::alert($r, true);
    return true;
  }

  public function prepareLoadTreeById($id, $depth = null){
    if(!$this->haveRootTable()){
      throw new \Exception('Object not loaded');
      return false;
    }
    if(!$this->getRootTable()->isTree()){
      throw new \Exception('Trying to load tree on a object that don\'t behave as nested set.');
      return false;
    }
    $behavior_name = 'nestedset';
    $behavior = $this->getRootTable()->getBehavior();
    $lft_key = $behavior->getFieldName($behavior_name, 'lft');
    $rgt_key = $behavior->getFieldName($behavior_name, 'rgt');
    $level_key = $behavior->getFieldName($behavior_name, 'level');
    $root_id_key = $behavior->getFieldName($behavior_name, 'root_id');
    $root_alias = $this->getRootAlias();
    $root_table_name = $this->getRootTable()->getTableName();

    $root_id_field = $behavior->hasManyRoots()?", $root_id_key":'';
    $this->subQuerySql("SELECT $lft_key , $rgt_key, $level_key $root_id_field FROM $root_table_name WHERE id = ?", 'subquery', array(array('type'=>'int', 'value'=>$id)));

    $this->andWhere("$root_alias.$lft_key >= subquery.$lft_key");
    $this->andWhere("$root_alias.$rgt_key <= subquery.$rgt_key");
    if($behavior->hasManyRoots()){
      $this->andWhere("$root_alias.$root_id_key = subquery.$root_id_key");
    }
    if($depth && \is_numeric($depth)){
      $this->andWhere("$root_alias.$level_key <= subquery.$level_key + $depth");
//      $this->andWhere("$root_alias.$level_key <= subquery.$level_key + ?", $depth);// _predicate($k, $v) don't recognize ? so bind manually
//      $this->setBindParam(array('type' => 'int', 'value' => $depth));
    }
    $this->orderBy("$root_alias.$lft_key");


    // add select:
    $this->addSelect("$root_alias.$lft_key, $root_alias.$rgt_key, $root_alias.$level_key");

    return true;
  }
  public function prepareLoadTreeByRootId($root_id, $depth = null){
    if(!$this->haveRootTable()){
      throw new \Exception('Object not loaded');
      return false;
    }
    if(!$this->getRootTable()->isTree()){
      throw new \Exception('Trying to load tree on a object that don\'t behave as nested set.');
      return false;
    }
    $behavior_name = 'nestedset';
    $behavior = $this->getRootTable()->getBehavior();
    if(!$behavior->hasManyRoots()){
      throw new \Exception("This nestedset don't have root_id defined (add to definition 'hasManyRoots'=>true).");
      return false;
    }
    $lft_key = $behavior->getFieldName($behavior_name, 'lft');
    $rgt_key = $behavior->getFieldName($behavior_name, 'rgt');
    $level_key = $behavior->getFieldName($behavior_name, 'level');
    $root_id_key = $behavior->getFieldName($behavior_name, 'root_id');
    $root_alias = $this->getRootAlias();
    $root_table_name = $this->getRootTable()->getTableName();

    $this->andWhere("$root_alias.$lft_key >= 1");
    $this->andWhere("$root_alias.$root_id_key = $root_id");
//    $this->andWhere("$root_alias.$rgt_key <= subquery.$rgt_key");
    if($depth && \is_numeric($depth)){
      $this->andWhere("$root_alias.$level_key <= $depth");
    }
    $this->orderBy("$root_alias.$lft_key");

    // add select:
    $this->addSelect("$root_alias.$lft_key, $root_alias.$rgt_key, $root_alias.$level_key");

    return true;
  }

  /**
   *
   * @param mixed $path
   * @param int $depth
   * @param mixed $in [NOT IMPLEMENTED]
   * @return mixed
   * @todo implement $in condition
   */
  public function prepareLoadTreeByPath($path, $depth = null, $in = null){
    if(!$this->haveRootTable()){
      throw new \Exception('Object not loaded');
      return false;
    }
    if(!$this->getRootTable()->isTree()){
      throw new \Exception('Trying to load tree on a object that don\'t behave as nested set.');
      return false;
    }
    $behavior_name = 'nestedset';
    $behavior = $this->getRootTable()->getBehavior();

    $lft_key = $behavior->getFieldName($behavior_name, 'lft');
    $rgt_key = $behavior->getFieldName($behavior_name, 'rgt');
    $level_key = $behavior->getFieldName($behavior_name, 'level');
    $root_id_key = $behavior->getFieldName($behavior_name, 'root_id');
    $path_key = $behavior->getPathField();
    $root_alias = $this->getRootAlias();
    $root_table_name = $this->getRootTable()->getTableName();

    $root_id_field = $behavior->hasManyRoots()?", $root_id_key":'';
//    $this->subQuerySql("SELECT $lft_key , $rgt_key, $level_key $root_id_field FROM $root_table_name WHERE id = ?", 'subquery', array(array('type'=>'int', 'value'=>$id)));
//
//    $this->andWhere("$root_alias.$lft_key >= subquery.$lft_key");
//    $this->andWhere("$root_alias.$rgt_key <= subquery.$rgt_key");
//    if($behavior->hasManyRoots()){
//      $this->andWhere("$root_alias.$root_id_key = subquery.$root_id_key");
//    }
//    if($depth && \is_numeric($depth)){
//      $this->andWhere("$root_alias.$level_key <= subquery.$level_key + $depth");
////      $this->andWhere("$root_alias.$level_key <= subquery.$level_key + ?", $depth);// _predicate($k, $v) don't recognize ? so bind manually
////      $this->setBindParam(array('type' => 'int', 'value' => $depth));
//    }
//    $this->orderBy("$root_alias.$lft_key");
//
//    // add select:
//    $this->addSelect("$root_alias.$lft_key, $root_alias.$rgt_key, $root_alias.$level_key");

//    return true;

//  $path = 'H/F/C';
//  $in = null;
    if(\is_string($path)){
      $value = \trim($path, "/");
      $path = explode('/', $value);
    }

    $path = array_values($path);
    //\hat\dbg::alert($path);

    $sql = '';

    foreach($path as $k => $p){
      $f = 'f_' . $k;
      $select = "$f.id, $f.$lft_key, $f.$rgt_key, $f.$level_key, $f.$root_id_key";
      $last_select = "$f.id, $f.$path_key";
      //$select = "$f.*";

      //$v = "'$p'";
      $v = '?';

      if($k == \count($path) - 1){
        $select = $last_select;
      }
      if($k == 0){

        $_in_where = '';
        if(FALSE && $in){
          $in_lft = null;
          $in_rgt = null;
          $in_level = null;
          $_in_where = " $f.$lft_key > $in_lft AND $f.$rgt_key < $in_rgt AND $f.$level_key = $in_level + 1 AND ";
        }

        $sql = "SELECT $select FROM $root_table_name $f WHERE $_in_where $f.$path_key = $v AND $f.$lft_key IS NOT NULL ORDER BY $f.$level_key, $f.$lft_key";
      }else{
        $f_previus = 'f_' . ($k-1);
        $sql = "SELECT $select FROM $root_table_name $f, ( ". $sql . " ) AS $f_previus WHERE $f.$path_key = $v AND $f.$lft_key > $f_previus.$lft_key AND $f.$rgt_key < $f_previus.$rgt_key AND $f.$level_key = $f_previus.$level_key + 1 AND $f.$root_id_key = $f_previus.$root_id_key";
      }
      $this->setBindParam($p);
    }

    //\hat\dbg::alert($sql);
    $this->setSql($sql);

    return true;
  }


  function test(){
    //print_r($this->_table_path);
    $d = array(
        'UPDATE' => $this->_UPDATE,
        'SET' => $this->_SET,
        'SELECT' => $this->_SELECT,
        'FROM' => $this->_FROM,
        'JOIN' => $this->_JOIN,
        'WHERE' => $this->_WHERE,
        'BIND_PARAMS' => $this->_BIND_PARAMS,
        'BIND_WHERE_PARAMS' => $this->_BIND_WHERE_PARAMS,
        '__sql__' => $this->_sql
    );

    print_r($d); exit;
  }
}
?>