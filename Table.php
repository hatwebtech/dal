<?php
/**
 * Abstract class inherited by classes that defines database tables
 * Methods in this class encapsulate databese calls created by DalQuery class
 * where all sql should go so that querys could be rewriten for specific database
 * server.
 *
    Copyright 2011 hatwebtech.com

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
 * @author Panajotis Zamos
 * @method Table setFoo($value) set value for column name "foo"
 * @method mixed|null getFoo() return value for column name "foo" or oasMany/haasOne association named "foo"
 * @method bool getFooExists() return true if column "foo" exists in result set
 * @method int getFooCount() return count for hasMany association named "foo"
 * @method Table addBar() add association "bar"
 * @method bool loadOneByColumnName($value) true if object with column_name == $value found
 * @method array loadByColumnName($value) array of Table objects
 */
namespace hat\dal;
abstract class Table{

  private $_columns = array();
  private $_results = array();
  private $_result_query;
  private $_query;

  private $_for_save = array(); // save this fields
  private $_saved = array(); // save this fields
  private $_old_values = array(); //
  private $_has_one_for_save = array(); // save this objects
  private $_has_many_for_save = array(); // save this objects

  protected $behavior = null; // \hat\dal\Behavior object (if loaded)
  private $_behaviors = array(); //

  private $_associations = array();
  private $_primaryKeys = array();

  private $_tableName;
  private $_tableAlias;
  private $_is_loaded = false;

  public function  __construct() {
    $this->setTableDefinition();
    $this->setUp();
    //$this->_behave('DalQuery');
    // load behavior on table init
    $this->behavior = new\hat\dal\Behavior($this, $this->_behaviors);
  }
  public function __destruct() {
    unset($this->behavior);
  }

  private function _loaded(){
    $this->_is_loaded = true;
  }
  /**
   * return true if Table object is loaded otherway false
   * @return bool
   */
  public function isLoaded(){
    return $this->_is_loaded;
  }

  /*
   * data (tables) config
   */


  public function setAlias($alias) {
    $this->_tableAlias = $alias;
  }
  public function getAlias() {
    return $this->_tableAlias;
  }
  /**
   *
   * @return DalQuery $query
   */
  public function getQuery() {
//    \hat\dbg::alert($this->_query);
//    \hat\dbg::alert($this->_query);
    return $this->_query;
  }
  public function setQuery(&$query) {
    $this->_query = $query;
 //   \hat\dbg::alert($this->_query);
  }

  /**
   *
   * @return Behavior
   */
  public function getBehavior(){
    if(!isset($this->behavior)){
      if(!empty($this->_behaviors)){
        $this->behavior = new\hat\dal\Behavior($this, $this->_behaviors);
      }
    }
    if(isset($this->behavior)){
      return $this->behavior;
    }
    return false;
  }

  /**
   * set database table name (used in table definition class)
   * @param string $name
   */
  protected function setTableName($name){
    $this->_tableName = $name;
  }
  /**
   *
   * @param bool $db_table if true return database table name otherway return model name
   * @param bool $with_namespace return full name (with namespace)
   * @return string
   */
  public function getTableName($db_table = true, $with_namespace = false){
    if($db_table){
      return $this->_tableName;
    }
    $name = get_class($this);
    if($with_namespace){
      return $name;
    }
    $name_parts = \explode(DAL::getNamespaceSeparator(), $name);
    return \end($name_parts);
  }
  /**
   * used in table definition class
   */
  abstract protected function setTableDefinition();

  /**
   *  define column in table
   *
   * @param string $name
   * @param string $type (integer, string, timestamp, boolean)
   * @param int $size
   * @param mixed $options
   */
  public function hasColumn($name, $type, $size = 1, $options = array()){
    $column = array(
        'name' => $name,
        'type' => $type,
        'size' => $size,
        'options' => $options
    );

    $this->_columns[$name] = $column;
    if(isset($column['options']['primary']) && $column['options']['primary']){
      $this->_primaryKeys[] = $name;
    }
  }

  /**
   * return array of column name that are defined as primery keys
   * @return mixed
   */
  public function getPrimaryKeys(){
    return $this->_primaryKeys;
  }

  /**
   * return array of column names defined for this table
   * @return mixed
   */
  public function getColumns(){
    $columns = array();
    foreach($this->_columns as $column_name => $column_info){
      $columns[] = $column_name;
    }

    return $columns;
  }

  /**
   * Return information for specific column (type, size, options)
   * @param string $name column name
   * @param string $info one of type, size or options
   * @return string|mixed|false string or array if info found, false if not.
   */
  public function columnInfo($name, $info = 'type'){
    if(isset($this->_columns[$name]) && isset($this->_columns[$name][$info])){
      return $this->_columns[$name][$info];
    }
    return false;
  }
  /**
   * Return association info
   * @param string $name association name (alias name if used)
   * @return mixed|false
   */
  public function associationInfo($name){
    if(isset($this->_associations[$name])){
      return $this->_associations[$name];
    }
    return false;
  }

  public function setResults($results){
    $this->_results = $results;
    if($this->behavior){
      $this->behavior->resetTable($this);
    }
    $this->_loaded();
  }

  public function setResultQuery($result_query){
    $this->_result_query = $result_query;
  }

  public function __call($name, $arguments){
    // set & get
    $name_part_1 = \substr($name, 0, 3);
    $name_part_2 = \substr($name, 3);
    if($name_part_1 == 'set'){
      return $this->set($name_part_2, $arguments);
    }
    if($name_part_1 == 'get'){
      return $this->get($name_part_2, $arguments);
    }
    if($name_part_1 == 'add'){
      return $this->add($name_part_2, $arguments);
    }

    // load
    $name_part_1 = \substr($name, 0, 6);
    $name_part_2 = \substr($name, 6);

    if($name_part_1 == 'loadBy'){
      return $this->loadBy($name_part_2, $arguments);
    }

    $name_part_1 = \substr($name, 0, 9);
    $name_part_2 = \substr($name, 9);

    if($name_part_1 == 'loadOneBy'){
      return $this->loadOneBy($name_part_2, $arguments);
    }

    // other magic methods

  }

  public function set($name, $arguments){
    $org_name = $name;
    $name_parts = \explode('_', $name);
    $_name = '';
    foreach($name_parts as $name_part){
      $_name .= \ucfirst($name_part);
    }
    $name = $_name;
    if(!\is_array($arguments)){
      $arguments = array($arguments);
    }
    if(array_key_exists(0, $arguments)){
      $value = $arguments[0];
    }else{
      \trigger_error('Value for ' . $name . ' not passed.', \E_USER_WARNING);
      return $this;
    }

    if(\is_object($value) || \is_array($value)){
      // enable setting objects???
      \trigger_error('Invalid type passed for ' . $name . ' [object or array passed].', \E_USER_WARNING);
      return $this;
    }

    // !== false
    if($this->get($name . 'Exists', $arguments)){
      $old_value = $this->get($name, $arguments);
      if($value == $old_value){
        // value didn't changed
        return $this;
      }
      //print_r($old_value);
      $this->_old_values[$name] = $old_value;
    }

    // field was not in result or is new so set it
    //$this->_for_save[$name] = $value;
    $this->_for_save[$org_name] = $value;
    return $this;
  }

  public function getForSave($name){
    $value = null;
    if(isset($this->_for_save[$name])){
      $value = $this->_for_save[$name];
    }
    return $value;
  }
  public function unsetForSave($keys){
    if(!\is_array($keys)){
      $keys = array($keys);
    }
    foreach($keys as $key){
      unset($this->_for_save[$key]);
    }
  }

  public function getOldValue($name){
    $value = null;
    if(isset($this->_old_values[$name])){
      $value = $this->_old_values[$name];
    }
    return $value;
  }

  public function get($name, $arguments = array()){
    //return $this->_results;
    //return $this->_columns;
    //return $this->_associations;

    // search in cloumns
    foreach($this->_columns as $k=>$v){
      $k_parts = \explode('_', $k);
      $k_name = '';
      foreach($k_parts as $k_part){
        $k_name .= \ucfirst($k_part);
      }

      if($name == $k_name . 'Exists' || $name == $k . 'Exists'){
        if(\array_key_exists($k, $this->_results)){
          return true;
        }else{
          return false;
        }
      }

      if($name == $k_name || $name == $k){
        // NULL ???
        if(\array_key_exists($k, $this->_results)){
          return $this->_results[$k];
        }elseif(\array_key_exists($k_name, $this->_results)){
          return $this->_results[$k_name];
        }elseif(\array_key_exists($k, $this->_for_save)){
          return $this->_for_save[$k];
        }elseif(\array_key_exists($k_name, $this->_for_save)){
          return $this->_for_save[$k_name];
        }else{
          //\hat\dbg::level(2);
          \hat\dbg::alert(compact('name', 'k_name', 'k'));
          \hat\dbg::alert($this->_for_save);
          \hat\dbg::alert($this->_results);
          echo "<pre>\n\n";
          \trigger_error($name . ' not found in this result set.', \E_USER_WARNING);
          throw new \Exception($name . ' not found in this result set.');
          return null;
        }
      }
    }
    // search in associations
    /**
     * enable getBlogs(37); where Blogs is assoc alias
     */
    if(isset($this->_associations[$name])){

      if(isset($this->_associations[$name]['type'])){
        if($this->_associations[$name]['type'] == 'hasOne'){
          if(isset($this->_has_one_for_save[$name])){
            return $this->_has_one_for_save[$name];
          }elseif(isset($this->_results[$name])){
            $assoc = $this->_result_query->getTable($name);
            $assoc->setResultQuery($this->_result_query);
            $assoc->setResults($this->_results[$name]);
              // check this assoc for save upon $this->save()
            $this->_has_one_for_save[$name] = $assoc;
            return $assoc;
          }else{
            // this assoc is added by get()
            if(isset($this->_result_query)){
              $assoc = $this->_result_query->getTable($name);
            }else{
              $assoc = \hat\dal\DAL::load($name);
            }
            $assoc->setResultQuery($this->_result_query);
            $assoc->setResults(array());
            $this->_has_one_for_save[$name] = $assoc;
            return $this->_has_one_for_save[$name];
          }
        }elseif($this->_associations[$name]['type'] == 'hasMany'){
          if(isset($arguments) && isset($arguments[0]) && \is_numeric($arguments[0])){
            if(isset($this->_has_many_for_save[$name]) && isset($this->_has_many_for_save[$name][$arguments[0]])){
              return $this->_has_many_for_save[$name][$arguments[0]];
            }elseif(isset($this->_results[$name]) && isset($this->_results[$name][$arguments[0]])){
              $_assoc = $this->_result_query->getTable($name);
              $assoc = clone $_assoc;
              $assoc->setResultQuery($this->_result_query);
              //$assoc = \hat\dal\DAL::load($name);
              //print_r($assoc); exit;
              $assoc->setResults($this->_results[$name][$arguments[0]]);
              // check this assoc for save upon $this->save()
              if(!isset($this->_has_many_for_save[$name])){
                $this->_has_many_for_save[$name] = array();
              }
              $this->_has_many_for_save[$name][$arguments[0]] = $assoc;
//              print_r($assoc->toArray());
              return $assoc;
            }else{
              if(isset($this->_result_query)){
//                print_r('result_query');
                $_assoc = $this->_result_query->getTable($name);
                $assoc = clone $_assoc;
              }else{
//                print_r('dal::load');
                $assoc = \hat\dal\DAL::load($name);
              }
              $assoc->setResultQuery($this->_result_query);
              // this assoc is added by get(_new_index_) so add it to _has_many_for_save[]
              $assoc->setResults(array());
              if(!isset($this->_has_many_for_save[$name])){
                $this->_has_many_for_save[$name] = array();
              }
              $this->_has_many_for_save[$name][$arguments[0]] = $assoc;
              return $this->_has_many_for_save[$name][$arguments[0]];
              //throw new \Exception("Invalid index {$arguments[0]} for $name association.");
            }
          }else{
            throw new \Exception("Missing index for $name association.");
          }
        }else{
          throw new \Exception('Unknown type in associations info.');
        }
      }else{
        throw new \Exception('No type in associations info.');
      }



      $assoc->setResults(array());
      return $assoc;
    }

    // search for association count
    foreach($this->_associations as $k=>$v){
      if($name == $k.'Count'){

        if($v['type'] == 'hasMany'){
          $result_count = 0;
          if(isset($this->_results[$k])){
            $result_count = \count($this->_results[$k]);
          }
          $for_save_count = 0;
          if(isset($this->_has_many_for_save[$k])){
            $for_save_count = \count($this->_has_many_for_save[$k]);
          }

          return $result_count + $for_save_count;
        }else{
          // hasOne
          if(isset($this->_results[$k]) || isset($this->_has_one_for_save[$k])){
            return 1;
          }
          return 0;
        }

      }

      if($name == $k.'Exists'){
        // if count is 0 then it does not exists :)
        return ($this->get($k.'Count', $arguments))?true:false;

//        if(isset($this->_results[$k])){
//          if($v['type'] == 'hasMany'){
//            if(isset($arguments) && isset($arguments[0]) && \is_numeric($arguments[0])){
//              if(isset($this->_results[$k][$arguments[0]])){
//                return true;
//              }
//            }
//            return false;
//          }else{
//            return true;
//          }
//        }else{
//          return false;
//        }
      }
    }

    \trigger_error($name . ' not found.', \E_USER_WARNING);
    return null;
  }

  public function toArray(){
    return $this->_results + $this->_for_save;
  }

  public function add($name, $arguments = array()){
    if(!isset($this->_has_many_for_save[$name])){
      $this->_has_many_for_save[$name] = array();
    }
    // count in results
    $_has_many_in_results_count = $this->get($name . 'Count', $arguments);
//    $_has_many_in_results_count = $this->get($name . 'Count', $arguments);
//    // count in _has_many_for_save
//    $_has_many_for_save_count = \count($this->_has_many_for_save[$name]);
    $_has_many_for_save_count = 0;
    // $i = $c1 + $c2
//    $i = $_has_many_in_results_count + $_has_many_for_save_count;
    $i = $_has_many_in_results_count ;
//    print_r("i= $i = $_has_many_in_results_count + $_has_many_for_save_count #338<br>\n");;
    return $this->get($name, array($i));
  }

  public function behave($at, &$query){
    if($query->useBehavior()){
      return $this->_behave($at, $query);
    }else{
      //return true;
    }
  }
  private function _behave($at, &$query = null){
    if(isset($this->behavior)){
      if($query){
        $this->behavior->setQuery($query);
      }
      return $this->behavior->act($at);
    }else{
      if(!empty($this->_behaviors)){
        $this->behavior = new\hat\dal\Behavior($this, $this->_behaviors);
        if($query){
          $this->behavior->setQuery($query);
        }
        return $this->behavior->act($at);
      }
    }
  }

  public function isNew(){
    $new = false;
    // if primary keys not set it's new
    if(empty($this->_primaryKeys)){
      $new = true;
      //\hat\dbg::alert("it's new");
    }
    // if primary keys not set it's new
    foreach($this->_primaryKeys as $key){
      if(!\array_key_exists($key, $this->_results)){
        $new = true;
        //\hat\dbg::alert("it's new : $key");
      }
    }
    return $new;
  }
  // save
  /**
   * Save new object or edited one
   * @param string $return column name to return
   * @return null|mixed
   */
  public function save($return = null){
    //\hat\dbg::alert($this->_tableName);
    //\hat\dbg::alert($this->_behaviors);
    //\hat\dbg::alert($this->behavior, true);
    //\hat\dbg::alert('k', true);
    //\hat\dbg::alert($this->toArray(), true);
    $this->_behave('pre_save');
    $this->preSave();
  //          throw new \Exception("$name association can not be modified from this model [its hasOne association].");

    foreach($this->_associations as $assoc_name => $assoc_info){
      if($assoc_info['type'] == 'hasOne'){
        if(isset($this->_has_one_for_save[$assoc_name])){
          $local_key = $assoc_info['keys']['local'];
          $local_value = $this->_has_one_for_save[$assoc_name]->save($assoc_info['keys']['foreign']);
          if($local_value){
            // hasOne was inserted so it's need to be saved.
            $k_name = $this->_getHumanFieldName($local_key);
            $this->set($k_name, $local_value);
          }
        }
      }
    }

    $returned = null;
    if(empty($this->_for_save)){
      // nothing to save.
    }else{
      // primary key? insert or update ???
      if(empty($this->_primaryKeys)){
        $primary_key_set = false;
      }else{
        $primary_key_set = true;
      }
      foreach($this->_primaryKeys as $primary_key){
          $primary_key_set = $primary_key_set && $this->get($this->_getHumanFieldName($primary_key) . 'Exists', array());
      }
      if($primary_key_set){
        // update
        $this->_update();
      }else{
        // insert and return $return keys
//        \hat\dbg::alert($this->_for_save);
        $returned = $this->_insert($return);
      }
      $this->_loaded();

    }

//    \hat\dbg::alert($this->toArray()); exit;

    //print_r($this->_has_many_for_save, true);
//    print_r($this->toArray());
//    if($this->getTableName() == 'hat_module'){
//      print_r(array($this->getTableName() => array('results' => $this->_results, 'for_save' => $this->_for_save)));
//    }
//    print_r($this->_for_save);
//    print_r($this->getId(), true);
    foreach($this->_associations as $assoc_name => $assoc_info){
      if($assoc_info['type'] == 'hasMany'){
        if(isset($this->_has_many_for_save[$assoc_name])){
          $local_key = $assoc_info['keys']['local'];
          // when adding, primary key is seted from _insert and does not 'Exists'
//          if($this->get($this->_getHumanFieldName($local_key) . 'Exists', array())){
            //$local_value = $this->get($this->_getHumanFieldName($local_key), array());
            $local_value = $this->get($local_key, array());
//          }else{
//            \trigger_error($local_key . ' do not exists in this results.', \E_USER_WARNING);
//          }
          $foreign_key = $this->_getHumanFieldName($assoc_info['keys']['foreign']);
//          if($foreign_key == 'ItemId'){
//            foreach($this->_has_many_for_save[$assoc_name] as $has_many_assoc){
//              \hat\dbg::alert($has_many_assoc->toArray());
//            }
//          }
          foreach($this->_has_many_for_save[$assoc_name] as $has_many_assoc){
            $assoc_a = $has_many_assoc->toArray();
            if(empty($assoc_a)){
              continue;
            }
            $has_many_assoc->set($foreign_key, $local_value);
//            print_r($has_many_assoc->toArray());
            $has_many_assoc->save();
          }
        }
      }
    }

    $this->postSave();
    $this->_behave('post_save');
    return $returned;
  }

  public function refreshResults($new_result = null){
    if(!isset($new_result)){
      $new_result = $this->_for_save;
      foreach($new_result as $k=>$v){
        $this->_results[$this->_getColumnName($k)] = $v;
        unset($this->_for_save[$k]);
      }
      $new_result = $this->_saved;
      foreach($new_result as $k=>$v){
        $this->_results[$this->_getColumnName($k)] = $v;
      }
    }else{
      foreach($new_result as $k=>$v){
        $this->_results[$k] = $v;
        unset($this->_for_save[$k]);
      }
    }
//    \hat\dbg::alert($this->_for_save);
//    \hat\dbg::alert($this->_results);
  }

  /**
   *
   * @param string $return column name to return
   * @return null|mixed
   */
  private function _insert($return = null){
      //\hat\dbg::alert($this->_for_save);
    $this->preInsert();
    // use _result_query ?
    $this->_query = \hat\dal\DAL::query();
    $this->_query->insertInto($this->_getModelName());
    foreach($this->_for_save as $k=>$v){
      $this->_query->value($this->_getColumnName($k), $v);

      //unset($this->_for_save[$k]);
    }

    if(!isset($this->_primaryKeys[0])){
      throw new \Exception('No primary key for ' . $this->_getModelName());
    }
    if($return){
      $this->_primaryKeys[] = $return;
      //$this->_query->returning($return);
    }
    $returns = \implode(', ', $this->_primaryKeys);
    $this->_query->returning($returns);

    $r1 = $this->_query->queryStmt();
    if($r1 === false){
      $r1 = $this->_query->getLastPdoErrorMessage();
      print_r($this->_query->queryDebug());
      \hat\dbg::alert($this->_for_save);
      //$r1 = $this->_query->getPdoErrorMessages();
      throw new \Exception('Error saving [insert] ' . $this->_getModelName() . ' with message: ' . $r1);
    }

    $this->_saved = $this->_for_save;
    $this->_for_save = array();
    
    $this->refreshResults();

    if($r1 === 0){
//      print_r($this->_query->queryDebug());
//      echo '<pre>';
//      print_r($this->_query->debugParams());
//      echo '</pre>';
    }

    //$r1 = $this->_query->getResults();
    $r1 = $this->_query->getResultsDbRow();
//    print_r($r1); exit;

    foreach($this->_primaryKeys as $primary_key){
      if(isset($r1[0][$primary_key])){
        $this->set($this->_getHumanFieldName($primary_key), $r1[0][$primary_key]);
        $this->_results[$primary_key] = $r1[0][$primary_key];
      }
    }

    $this->postInsert();
    if($return){
      if(isset($r1[0]) && isset($r1[0][$return])){
        return $r1[0][$return];
      }
    }

    return null;


  }

  private function _update(){
    $this->preUpdate();
    // use _result_query ?
    $this->_query = \hat\dal\DAL::query();
    $this->_query->update($this->_getModelName());
    //\hat\dbg::alert($this->_for_save);
    foreach($this->_for_save as $k=>$v){
      //\hat\dbg::alert($this->_getColumnName($k));
      $this->_query->set($this->_getColumnName($k), $v);
    }
    foreach($this->_primaryKeys as $primary_key){
      if($this->get($this->_getHumanFieldName($primary_key) . 'Exists', array())){
        $this->_query->andWhere("$primary_key =?", $this->get($this->_getHumanFieldName($primary_key), array()));
      }else{
        throw new \Exception('Error saving [update] ' . $this->_getModelName() . '; ' . $primary_key . ' not set.');
        return;
      }
    }
    //\hat\dbg::alert($this->_query->queryDebug()); exit;
    $r1 = $this->_query->queryStmt();
    if($r1 === false){
      $r1 = $this->_query->getLastPdoErrorMessage();
      //$r1 = $this->_query->getPdoErrorMessages();
      throw new \Exception('Error saving [update] ' . $this->_getModelName() . ' with message: ' . $r1);
    }

    $this->refreshResults();

    $this->postUpdate();
    return null;

  }

  private function _getHumanFieldName($name){
    $k_parts = \explode('_', $name);
    $k_name = '';
    foreach($k_parts as $k_part){
      $k_name .= \ucfirst($k_part);
    }
    return $k_name;
  }

  private function _getModelName(){
    $full_class_name = \get_class($this);
    $class_name = substr($full_class_name, strrpos($full_class_name, '\\')+1);
    return $class_name;
  }

  private function _getColumnName($name){
    foreach($this->_columns as $k => $v){
      if($name == $k){
        return $k;
      }
      if($name == $this->_getHumanFieldName($k)){
        return $k;
      }
    }
  }

  /**
   *  delete
   * @todo unset from $this->_results
   * @todo unset from $this->_has_many_for_save
   */
  public function delete(){
    if($this->_behave('pre_delete') === false){
      //echo 'pre_delete exit';
      //return false; // is FALSE right response in case of skiping delete???
    }
    $this->preDelete();
    if(empty($this->_primaryKeys)){
      echo 'no primary keys';
      return false;
    }
    if(!isset($this->_query)){
      $this->_query = \hat\dal\DAL::query();
    }
    $this->_query->delete($this->_getModelName());
    foreach($this->_primaryKeys as $primary_key){
      if($this->get($this->_getHumanFieldName($primary_key) . 'Exists', array())){
        $this->_query->andWhere("$primary_key =?", $this->get($this->_getHumanFieldName($primary_key), array()));
      }else{
        throw new \Exception('Error deleting ' . $this->_getModelName() . '; ' . $primary_key . ' not set.');
      }
    }
    //print_r($this->_query->queryDebug()); exit;
    $r1 = $this->_query->queryStmt();
    if($r1 === false){
      $r1 = $this->_query->getLastPdoErrorMessage();
      //$r1 = $this->_query->getPdoErrorMessages();
      throw new \Exception('Error deleting ' . $this->_getModelName() . ' with message: ' . $r1);
      return false;
    }
    $this->postDelete();
    $this->_behave('post_delete');

    return true;

  }

  protected function hasOne($assoc_name, $keys = array(), $type = 'hasOne'){
    $_alias = $assoc_name;
    $_as_pos = \strpos($assoc_name, ' as ');
    if($_as_pos !== false){
      $_alias = \trim(\substr($assoc_name, $_as_pos + 4));
      $assoc_name = \trim(\substr($assoc_name, 0, $_as_pos));
    }

    $assoc = array(
        'association_name' => $assoc_name,
        'association_alias' => $_alias,
        'type' => $type,
        'keys' => $keys
    );
    $this->_associations[$_alias] = $assoc;
//    if($type == 'hasOne'){
//      $this->_hasOne[$_alias] = $assoc;
//    }
//    if($type == 'hasMany'){
//      $this->_hasMany[$_alias] = $assoc;
//    }
  }

  protected function hasMany($assoc_name, $keys = array()){
    $this->hasOne($assoc_name, $keys, 'hasMany');
  }

  protected function setUp(){}
  protected function actAs($name, $options = array()){
    $this->_behaviors[$name] = $options;
  }

  /*
   * data processing
   */

  /**
   *
   * @param <type> $where
   * @param bool $assoc if TRUE will return associative array with ID as keys
   * @param <type> $select
   * @return array
   */
  public function getAll($where = null, $assoc = true, $select = '*', $no_behavior = false){
    //$this->_behave('pre_load');
    $return = array();
    $q = DAL::query()->select($select)->from($this->getTableName(false));

    if($no_behavior){
      //$q = DAL::query()->select('t.' . $select)->from($this->getTableName(false) . ' AS t');
      $q = $q->noBehavior();
    }else{
      //$q = DAL::query()->select($select)->from($this->getTableName(false));
    }

    //$q = $q->limit(550);


    if($where){
      $q = $q->setWhereCondition($where);
    }
    if(isset($this->_columns['id'])){
      $q = $q->orderBy('id');
    }
    if($this->getTableName(false) == 'ISTables'){
      $q = $q->orderBy('table_name');

    }
      //\hat\dbg::alert($q->queryDebug());
    $r = $q->queryStmt();
    if($q === false){
      // error loading
      throw new \Exception('Error: ' . $q->getLastPdoErrorMessage());
//      \hat\dbg::alert('Error: ' . $q->getLastPdoErrorMessage());
      return false;
    }
    $r = $q->getResults();
    if(!isset($r[0])){
      // object not found
      return $return;
    }

    if(!$assoc){
      return $r;
    }

    $single_pkey = false;
    if(count($this->_primaryKeys) == 1){
      $single_pkey = true;
      $pkey = $this->_primaryKeys[0];
    }
    if($single_pkey){
      foreach($r as $_r){
        if($single_pkey){
          $return[$_r[$pkey]] = $_r;
        }
      }
    }else{
      $return = $r;
    }
    return $return;
  }

  public function find($type = 'first', $options = array()){
    throw new \Exception('Not implemented');
  }
  public function loadBy($name, $value){
    throw new \Exception('Not implemented');
    $this->_loaded();
  }
  public function loadOneBy($name, $value, $as = DAL::AS_ARRAY, $join = array()){
    $this->_behave('pre_load');
    $q = DAL::query()->select('tt.*')->from($this->getTableName(false) . ' tt')->where("tt.$name =?", $value)->limit(1);
    if($join){
      foreach($join as $k=>$v){
        if(is_string($v)){
          $q = $q->leftJoin("tt.$v tt$v")->addSelect("tt$v.*");
        }
      }
    }
//    \hat\dbg::alert($this->getTableName(false));
//    \hat\dbg::alert($q->queryDebug());
    $r = $q->queryStmt();
    if($r === false){
      // error loading
      return false;
    }
    $r = $q->getResults($as);
    if(!isset($r[0])){
      // object not found
      return false;
    }
    $this->_results = $r[0];
    $this->_result_query = $q;
    $this->_loaded();
    $this->_behave('post_load');
    return true;
  }

  /**
   * methods for nested set
   */

  /**
   *
   * @return bool
   */
  public function isTree(){
    if($this->behavior && $this->behavior->isIt('nestedset')){
      return true;
    }
    return false;
  }

  public function isInTree(){
    if($this->isTree()){
      // check if lft and root_id are set
      $behavior_name = 'nestedset';
      $lft_key = $this->behavior->getFieldName($behavior_name, 'lft');
      $rgt_key = $this->behavior->getFieldName($behavior_name, 'rgt');
      $level_key = $this->behavior->getFieldName($behavior_name, 'level');
      $root_id_key = $this->behavior->getFieldName($behavior_name, 'root_id');

      if($this->get($lft_key . 'Exists')){
        if($this->get($root_id_key . 'Exists')){
          $root_id = $this->get($root_id_key);
          if($root_id > 0){
            return true;
          }
        }
      }
    }
    return false;
  }

  // ?
  public function createRoot(){
    $q = DAL::query();
    return $q->createRoot($this);
  }

  /**
   *
   * @param int $id
   * @param bool $asArray
   * @return string|array $path
   */
  public function getPath($id = null, $asArray = false){
    $table = $this->getTableName();
    $q = DAL::query();
    $options = array();
    if(!$asArray){
      $options['as_string'] = true;
    }
    $options['path_glu'] = $this->behavior->getPathGlue();
    $options['path_field'] = $this->behavior->getPathField();
    if(!isset($id)){
      if($this->isLoaded() && $this->isTree()){
        $id = $this->get('Id', array());
        $lft_colum_name = $this->behavior->getFieldName('nestedset', 'lft');
        $rgt_colum_name = $this->behavior->getFieldName('nestedset', 'rgt');
        if($lft_colum_name && $rgt_colum_name && $this->get($lft_colum_name . 'Exists', array()) && $this->get($rgt_colum_name . 'Exists', array())){
          $options['lft'] = $this->get($lft_colum_name, array());
          $options['rgt'] = $this->get($rgt_colum_name, array());
        }

      }else{
        throw new \Exception('Trying to get path without tree Id and without tree node loaded.');
      }
    }

    return $q->getPath($id, $table, $options);
  }
  
  
  /**
   * Load tree by id, path or root_id
   * @param mixed $with id, path or root_id
   * @param mixed $options key=>value options
   * @return mixed tree as array with __children or null if nothing found
   * @example
<code>
   $options = array(
     'depth' => 2,
     'root_id' => 4,
     'in' => 37, //or
     'in' => 'files\images\big', //or
     'in' => array('files', 'images', 'big'),
     'base_query' => $q // DalQuery object
   );
</code>
   */
  public function loadTree($with = null, $options = array()){
    $this->_behave('pre_load');
    //\hat\dbg::alert($this->isTree(), true);
    $depth = isset($options['depth'])?$options['depth']:null;
    if(isset($options['base_query'])){
      $base_query = $options['base_query'];
    }else{
      $base_query =  $this->_baseQuery();
    }
    if(isset($with)){
      if(\is_string($with) || \is_array($with)){
        return $this->loadTreeByPath($with, $depth, $base_query, $options);
      }elseif(\is_numeric($with)){
        return $this->loadTreeById($with, $depth, $base_query, $options);
      }else{
        throw new \Exception('Invalid loading type');
      }
    }else{
      // load tree over root_id?
      if(isset($options['root_id']) && \is_numeric($options['root_id'])){
        return $this->loadTreeByRootId($options['root_id'], $depth, $base_query, $options);
      }else{
        throw new \Exception('Missing parameters for loading tree.');
      }
    }
    return false;
  }
  /**
   *
   * @param int $id
   * @param int $depth number of levels to retrive or 0 (null) for whole tree
   * @param DalQuery $base_query
   * @param mixed $options
   * @return mixed tree as array with __children or null if nothing found

   */
  public function loadTreeById($id, $depth, $base_query, $options = array()){
    $base_query->prepareLoadTreeById($id, $depth);
    //return $base_query->queryDebug();
    $r = $base_query->queryStmt();
    if($r===false){
      $r = $base_query->getLastPdoErrorMessage();
    }else{
      $r = $base_query->getResults();
    }
    return $r;
  }
  /**
   *
   * @param int $root_id
   * @param int $depth number of levels to retrive or 0 (null) for whole tree
   * @param DalQuery $base_query
   * @param mixed $options
   * @return mixed tree as array with __children or null if nothing found
   */
  public function loadTreeByRootId($root_id, $depth, $base_query, $options = array()){
    $base_query->prepareLoadTreeByRootId($root_id, $depth);
    //return $base_query->queryDebug();
    $r = $base_query->queryStmt();
    if($r===false){
      $r = $base_query->getLastPdoErrorMessage();
    }else{
      $r = $base_query->getResults();
    }
    return $r;
  }
  /**
   *
   * @param mixed $path path string or array with string parts
   * @param int $depth number of levels to retrive or 0 (null) for whole tree
   * @param DalQuery $base_query
   * @param mixed $options
   * @return mixed tree as array with __children or null if nothing found
   */
  public function loadTreeByPath($path, $depth, $base_query, $options = array()){
    $base_query_clone = clone $base_query;
    $in = isset($options['in'])?$options['in']:null;
    $base_query->prepareLoadTreeByPath($path, $depth, $in);
    //return $base_query->getSql();
    $r = $base_query->queryStmt();
    if($r===false){
      $r = $base_query->getLastPdoErrorMessage();
    }else{
      $r = $base_query->getResults();
    }

    //return $r;
    // load by id from result
    if(isset($r[0]) && isset($r[0]['id'])){
      return $this->loadTreeById($r[0]['id'], $depth, $base_query_clone, $options);
    }else{
      $r = null;
    }

    return $r;
  }

  /**
   *
   * @return DalQuery
   */
  private function _baseQuery(){
    $q = DAL::query()->select('_tree.*')->from($this->getTableName(false) . ' _tree');
    //\hat\dbg::alert($q->getRootAlias(), true);
    return $q;
  }

  /**
   *
   */
  public function getRoots(){
    if($this->isTree()){
        //ok
    }else{
      throw new \Exception('Model not a tree.');
    }
    return $this->behavior->getRoots();
  }
  public function removeFromTree(){
    if($this->isTree() && $this->isInTree()){
        //ok
    }else{
      throw new \Exception('Node not in a tree.');
    }
    return $this->behavior->removeFromTree();
  }
  /**
   *
   * @param Table $destination
   */
  public function insertAsFirstChildOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      if($this->isTree() && $this->isInTree()){
        throw new \Exception('This node is already in tree.');
      }
      $behavior_name = 'nestedset';

      return $this->behavior->insertAsFirstChildOf($destination);
    }else{
      throw new \Exception('Destination must be a tree.');
    }
    return false;
  }
  /**
   *
   * @param Table $destination
   */
  public function insertAsLastChildOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      if($this->isTree() && $this->isInTree()){
        throw new \Exception('This node is already in tree.');
      }
      $behavior_name = 'nestedset';
      
      return $this->behavior->insertAsLastChildOf($destination);
    }else{
      throw new \Exception('Destination must be a tree.');
    }
    return false;
  }
  /**
   *
   * @param Table $destination
   */
  public function moveAsLastChildOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      if($this->isTree() && $this->isInTree()){
        return $this->behavior->moveAsLastChildOf($destination);
      }else{
        throw new \Exception('Node must be a tree.');
      }
    }else{
      throw new \Exception('Destination must be a tree.');
    }
    return false;
  }
  /**
   *
   * @param Table $destination
   */
  public function moveAsFirstChildOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      if($this->isTree() && $this->isInTree()){
        return $this->behavior->moveAsFirstChildOf($destination);
      }else{
        throw new \Exception('Node must be a tree.');
      }
    }else{
      throw new \Exception('Destination must be a tree.');
    }
    return false;
  }
  /**
   *
   * @param Table $destination
   */
  public function moveAsPrevSiblingOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      if($this->isTree() && $this->isInTree()){
        return $this->behavior->moveBefore($destination);
      }else{
        throw new \Exception('Node must be a tree.');
      }
    }else{
      throw new \Exception('Destination must be a tree.');
    }
    return false;
  }
  /**
   *
   * @param Table $destination
   */
  public function moveAsNextSiblingOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      if($this->isTree() && $this->isInTree()){
        return $this->behavior->moveAfter($destination);
      }else{
        throw new \Exception('Node must be a tree.');
      }
    }else{
      throw new \Exception('Destination must be a tree.');
    }
    return false;
  }


  public function isLeaf(){
    if($this->isTree() && $this->isInTree()){
      return $this->behavior->isLeaf();
    }else{
      throw new \Exception('Node must be a tree.');
    }
    return false;
  }
  public function isRoot(){
    if($this->isTree() && $this->isInTree()){
      return $this->behavior->isRoot();
    }else{
      throw new \Exception('Node must be a tree.');
    }
    return false;
  }

  // ?
  public function setBaseQuery($q){
    throw new \Exception('Not implemented');
  }
  // ?
  public function getBaseQuery($q){
    throw new \Exception('Not implemented');
  }
  // ?
  public function resetBaseQuery(){
    throw new \Exception('Not implemented');
  }



  /**
   * data automatization
   * those methods will be called before and after select, insert, update, save (insert and update) and delete
   * also before and after used beehaviors
   */

  protected function preSelect(){}
  protected function preSave(){}
  protected function preInsert(){}
  protected function preUpdate(){}
  protected function preDelete(){}
  protected function preBehavior(){}
  protected function postSelect(){}
  protected function postSave(){}
  protected function postInsert(){}
  protected function postUpdate(){}
  protected function postDelete(){}
  protected function postBehavior(){}
}
?>