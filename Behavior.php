<?php
namespace hatwebtech\dal;
/**
 * Behavior class will have methods for all behaviors suported by HDM
 *
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
 * @author Panajotis Zamos

 * @todo remove behavior fields from result?
 */
class Behavior {
  private $_b_name = null;
  private $_table = null;
  private $_behaviors = array();
  private $_bv = array();
  private $_used_behaviors = array();
  private $_query;
  private $_installed_behaviors = array(
      'timestampable' => array(
          'fields' => array(
              'created_at' => array('created_at', 'timestamp'),
              'updated_at' => array('updated_at', 'timestamp'),
              ),
          'format' => 'Y-m-d H:i:s', // 2011-07-31 02:02:56
          'hooks' => array('pre_save'),
      ),
      'softdelete' => array(
          'fields' => array(
              'deleted_at' => array('deleted_at', 'timestamp'),
              ),
          'format' => 'Y-m-d H:i:s', // 2011-07-31 02:02:56
          'hooks' => array('DalSelect', 'pre_delete',),
      ),
      'blameable' => array(
          'fields' => array(
              'created_by' => array('created_by', 'string', 255),
              'updated_by' => array('updated_by', 'string', 255),
              ),
          'default' => 'N/A', //
          'blame_var' => 'userId', //
          'hooks' => array('pre_save'),
      ),
      'nestedset' => array(
          'fields' => array(
              'lft' => array('lft', 'int', 8),
              'rgt' => array('rgt', 'int', 8),
              'level' => array('level', 'int', 8),
              'root_id' => array('root_id', 'int', 8),
              ),
          'field_conditions' => array('root_id' => 'hasManyRoots'),
          'hasManyRoots' => false,
          'path_field' => 'name',
          'path_glue' => '/',
          'hooks' => array('pre_delete', 'post_delete', 'pre_save'),
      ),
      'multitenant' => array(
          'fields' => array(
              'identifier' => array('site_id', 'int', 8),
              ),
          'identifier' => false,
          'allow_tenant_update' => false,
          'hooks' => array('init', 'DalSelect', 'DalDelete', 'DalInsert', 'DalUpdate', 'DalJoin', 'pre_save'),
      ),
//      'sluggable' => array(),
  );

  /**
   *
   * @param Table $table
   * @param mixed $behaviors
   */
  public function __construct(&$table, $behaviors) {
    $this->_table = $table;
    $this->_behaviors = $behaviors;
    $this->_initBehaviors();
  }
  public function resetTable(&$table){
    $this->_table = $table;
  }

  private function _initBehaviors(){
    foreach($this->_behaviors as $b_name => $b_options){
      $b_name = \strtolower($b_name);
      if(isset($this->_installed_behaviors[$b_name])){
        $this->_used_behaviors[$b_name] = array();
        foreach($this->_installed_behaviors[$b_name] as $option_name => $option){
          if(isset($b_options[$option_name])){
            $this->_used_behaviors[$b_name][$option_name] = $b_options[$option_name]; // option seted by user
          }else{
            $this->_used_behaviors[$b_name][$option_name] = $option; // default option
          }
          $this->_init_fields($this->_used_behaviors[$b_name]);
          // skip this check and allways call init hooks (let __call() to handle)?
          if(isset($this->_used_behaviors[$b_name]['hooks']) && \in_array('init', $this->_used_behaviors[$b_name]['hooks'])){
            $behavior_action = '__init_' . $b_name;
            $this->$behavior_action($this->_used_behaviors[$b_name]);
          }
        }
      }
    }
//    \hat\dbg::alert($this->_behaviors);
//    \hat\dbg::alert($this->_used_behaviors);
//    \hat\dbg::alert($this->_installed_behaviors);
//    \hat\dbg::alert($this->isIt('nestedset'));
//    \hat\dbg::alert('kraj', true);
  }
  
  public function act($at){
    if(!\in_array($at, array('pre', 'post', 'pre_load', 'post_load', 'pre_delete', 'post_delete', 'pre_save', 'post_save', 'DalQuery', 'DalJoin', 'DalSelect', 'DalDelete', 'DalInsert', 'DalUpdate'))){
      throw new \Exception("Behavior at $at not suported.");
    }

    $behived = false;
    foreach($this->_used_behaviors as $b_name => $b_option){
      // skip this check and allways call hooks (let __call() to handle)?
      if(isset($this->_used_behaviors[$b_name]['hooks']) && \in_array($at, $this->_used_behaviors[$b_name]['hooks'])){
        $behavior_action = '__' . $at . '_' . $b_name;
        $this->_b_name = $b_name;
        //\hat\dbg::alert($behavior_action);
        $r = $this->$behavior_action($this->_used_behaviors[$this->_b_name]);
        if($r !== false){
          $behived = true;
        }
      }
    }
    return $behived;
  }

  public function __call($name, $arguments) {
    //\hat\dbg::timmer("_____ $name())");
    if(\strpos($name, '__') === 0){
      // not found but continue
      return false;
    }else{
      throw new \Exception("Method <b>$name</b> don't exists in hat\\dal\\Behavior class. ");
    }
  }

  /**
   *
   * @param DalQuery $query
   */
  public function setQuery(&$query){
    $this->_query = $query;
  }

  /**
   *
   * @param string $name behavior name
   * @return bool
   */
  public function isIt($name){
    if(isset($this->_used_behaviors[$name])){
      return true;
    }
    return false;
  }

  /**
   *
   * @param string $behavior_name
   * @param string $name
   * @return string|false field name or false on error
   */
  public function getFieldName($behavior_name, $name){
    if(isset($this->_used_behaviors[$behavior_name]) && isset($this->_used_behaviors[$behavior_name]['fields']) && isset($this->_used_behaviors[$behavior_name]['fields'][$name])){
      return $this->_used_behaviors[$behavior_name]['fields'][$name][0];
    }
    return false;
  }

  private function _init_fields($options){
    if(isset($options['fields'])){
      $columns = $this->_table->getColumns();
      $field_conditions = isset($options['field_conditions'])?$options['field_conditions']:array();
      foreach($options['fields'] as $field_name => $field_info){
        $continue = true;
        if(isset($field_conditions[$field_name]) && isset($options[$field_conditions[$field_name]])){
          $continue = (bool)$options[$field_conditions[$field_name]];
        }
        if($continue && isset($field_info[0]) && isset($field_info[1])){
          if(!\in_array($field_info[0], $columns)){
            $type = $field_info[1];
            $_size = $_options = null;
            if(isset($field_info[2])){
              $_size = $field_info[2];
            }
            if(isset($field_info[3])){
              $_options = $field_info[3];
            }
            $this->_table->hasColumn($field_info[0], $type, $_size, $_options);
          }
        }
      }
    }
  }

  /*
   * nested set
   */
  public function hasManyRoots(){
    if(isset($this->_used_behaviors['nestedset'])){
      return $this->_used_behaviors['nestedset']['hasManyRoots'];
    }
    return false;
  }
  public function getRootId(){
    if(isset($this->_used_behaviors['nestedset'])){
      $root_id_key = $this->getFieldName('nestedset', 'root_id');
      if($this->_table->get($root_id_key . 'Exists')){
        return $this->_table->get($root_id_key);
      }
    }

    return false;
  }
  public function getPathField(){
    if(isset($this->_used_behaviors['nestedset'])){
      return $this->_used_behaviors['nestedset']['path_field'];
    }
    throw new \Exception("model not a nested set.");
    return false;
  }
  public function getPathGlue(){
    if(isset($this->_used_behaviors['nestedset'])){
      return $this->_used_behaviors['nestedset']['path_glue'];
    }
    throw new \Exception("model not a nested set.");
    return false;
  }

  private function _extract_nestedset_vars($destination = null, $for_move = true){
    $behavior_name = 'nestedset';
    $lft_key = $this->getFieldName($behavior_name, 'lft');
    $rgt_key = $this->getFieldName($behavior_name, 'rgt');
    $level_key = $this->getFieldName($behavior_name, 'level');
    $root_id_key = $this->getFieldName($behavior_name, 'root_id');

//      \hat\dbg::alert($this->_for_save);

    $delta = 2; // for insert
    if($this->_table->get('idExists')){
      $id = $this->_table->get('id');
    }
    if($this->_table->isInTree()){
      if($this->_table->get($root_id_key . 'Exists')){
        $root_id = $this->_table->get($root_id_key);
      }else{
        throw new \Exception("Missing node $lft_key column.");
        return false;
      }
      if($this->_table->get($lft_key . 'Exists')){
        $lft = $this->_table->get($lft_key);
      }else{
        throw new \Exception("Missing node $lft_key column.");
        return false;
      }
      if($this->_table->get($rgt_key . 'Exists')){
        $rgt = $this->_table->get($rgt_key);
      }else{
        throw new \Exception("Missing node $rgt_key column.");
        return false;
      }
      if($this->_table->get($level_key . 'Exists')){
        $level = $this->_table->get($level_key);
      }else{
        throw new \Exception("Missing node $level_key column.");
        return false;
      }
      $delta = $rgt-$lft+1;
    }
    if($destination){
      $dest_root_id = $destination->getBehavior()->getRootId();
      $dest_lft = $destination->get($lft_key);
      $dest_rgt = $destination->get($rgt_key);
      $dest_level = $destination->get($level_key);
    }
      //\hat\dbg::alert(array($dest_root_id, $root_id), true);
    if($destination && $for_move){
      if($root_id != $dest_root_id){
        throw new \Exception("node and destination not in the same tree.");
        return false;
      }

      $move_to_the_left = ($lft > $dest_lft)? true : false;
    }


    $err_msg = '';
    $model_name = $this->_table->getTableName(false);
    $query = DAL::query();
    $this->_table->setQuery($query);

    $vars = \compact(array('lft_key', 'rgt_key', 'level_key', 'root_id_key', 'dest_root_id', 'dest_lft', 'dest_rgt', 'dest_level', 'id', 'root_id', 'lft', 'rgt', 'level', 'delta', 'move_to_the_left', 'model_name', 'query', 'err_msg'));

    $this->_bv[$behavior_name] = $vars;
    return true;
  }
  public function removeFromTree(){
    if($this->_table->isTree() && $this->_table->isInTree()){
    }else{
      throw new \Exception('Node must be in a tree.');
      return false;
    }

    $bn = 'nestedset'; // behavior name
    if(!isset($this->_bv[$bn])){
      $this->_extract_nestedset_vars();
    }
    //\hat\dbg::alert($this->_bv[$bn], true);

    $border = $this->_bv[$bn]['rgt'];
    $end_transaction = false;
    if(!$this->_bv[$bn]['query']->isInTransaction()){
      $this->_bv[$bn]['query']->begin();
      $end_transaction = true;
    }
    // remove rom tree
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->update($this->_bv[$bn]['model_name'])
            ->set($this->_bv[$bn]['root_id_key'], 0)
            ->where("{$this->_bv[$bn]['lft_key']} >= ?", $this->_bv[$bn]['lft'])
            ->andWhere("{$this->_bv[$bn]['rgt_key']} <= ?", $this->_bv[$bn]['rgt'])
            ->andWhere("{$this->_bv[$bn]['root_id_key']} = ?", $this->_bv[$bn]['root_id'])
            ;
    $r_remove = $this->_bv[$bn]['query']->queryStmt();
    if($r_remove === false){
      $this->_bv[$bn]['err_msg'] .= $this->_bv[$bn]['query']->getLastPdoErrorMessage();
    }
    // reduce lft tree
    $this->_bv[$bn]['query']->reset();
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->update($this->_bv[$bn]['model_name'])
            ->set("{$this->_bv[$bn]['lft_key']} = {$this->_bv[$bn]['lft_key']} - ?", $this->_bv[$bn]['delta'])
            ->where("{$this->_bv[$bn]['lft_key']} >?", $border)
            ->andWhere("{$this->_bv[$bn]['root_id_key']} = ?", $this->_bv[$bn]['root_id']);
    //$r_left = $this->_bv[$bn]['query']->queryDebug();
    $r_reduce_lft = $this->_bv[$bn]['query']->queryStmt();
    if($r_reduce_lft === false){
      $this->_bv[$bn]['err_msg'] .= $this->_bv[$bn]['query']->getLastPdoErrorMessage();
    }
    // reduce rgt tree
    $this->_bv[$bn]['query']->reset();
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->update($this->_bv[$bn]['model_name'])
            ->set("{$this->_bv[$bn]['rgt_key']} = {$this->_bv[$bn]['rgt_key']} - ?", $this->_bv[$bn]['delta'])
            ->where("{$this->_bv[$bn]['rgt_key']} >?", $border)
            ->andWhere("{$this->_bv[$bn]['root_id_key']} = ?", $this->_bv[$bn]['root_id']);
    //$r_left = $this->_bv[$bn]['query']->queryDebug();
    $r_reduce_rgt = $this->_bv[$bn]['query']->queryStmt();
    $this->_bv[$bn]['query']->reset();
    if($r_reduce_rgt === false){
      $this->_bv[$bn]['err_msg'] .= $this->_bv[$bn]['query']->getLastPdoErrorMessage();
    }

    if(empty($this->_bv[$bn]['err_msg'])){
      //ok
      if($end_transaction){
        $this->_bv[$bn]['query']->commit();
      }
      return true;
    }else{
      if($end_transaction){
        $this->_bv[$bn]['query']->rollback();
      }
      return $this->_bv[$bn]['err_msg'];
      return false;
    }
  }
  /**
   *
   * @param Table $destination
   */
  public function insertAsFirstChildOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      if($this->_table->isTree() && $this->_table->isInTree()){
        throw new \Exception('This node is already in tree.');
        return false;
      }
    }else{
      throw new \Exception('Destination must be a tree.');
      return false;
    }

    $bn = 'nestedset'; // behavior name
    $this->_extract_nestedset_vars($destination, false);

    $border = $this->_bv[$bn]['dest_lft'];
    $lft = $this->_bv[$bn]['dest_lft'] + 1;
    return $this->_insertNode($border, $lft);
  }
  public function insertAsLastChildOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      if($this->_table->isTree() && $this->_table->isInTree()){
        throw new \Exception('This node is already in tree.');
        return false;
      }
    }else{
      throw new \Exception('Destination must be a tree.');
      return false;
    }

    $bn = 'nestedset'; // behavior name
    $this->_extract_nestedset_vars($destination, false);

    $border = $this->_bv[$bn]['dest_rgt'] - 1;
    $lft = $this->_bv[$bn]['dest_rgt'];
    return $this->_insertNode($border, $lft);
  }
  private function _insertNode($border, $lft){
    $bn = 'nestedset';
    $this->_bv[$bn]['query']->begin();

    //update lft
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->update($this->_bv[$bn]['model_name'])
            ->set("{$this->_bv[$bn]['lft_key']} = {$this->_bv[$bn]['lft_key']} + ?", $this->_bv[$bn]['delta'])
            ->where("{$this->_bv[$bn]['lft_key']} >?", $border)
            ->andWhere("{$this->_bv[$bn]['root_id_key']} = ?", $this->_bv[$bn]['dest_root_id']);
    //$r_left = $this->_bv[$bn]['query']->queryDebug();
    $r_left = $this->_bv[$bn]['query']->queryStmt();
    if($r_left === false){
      $this->_bv[$bn]['err_msg'] .= $this->_bv[$bn]['query']->getLastPdoErrorMessage();
    }
    //\hat\dbg::alert($r_left);


    //update rgt
    $this->_bv[$bn]['query']->reset();
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']
            ->update($this->_bv[$bn]['model_name'])
            ->set("{$this->_bv[$bn]['rgt_key']} = {$this->_bv[$bn]['rgt_key']} + ?", $this->_bv[$bn]['delta'])
            ->where("{$this->_bv[$bn]['rgt_key']} >?", $border)
            ->andWhere("{$this->_bv[$bn]['root_id_key']} = ?", $this->_bv[$bn]['dest_root_id']);
    //$r_right = $this->_bv[$bn]['query']->queryDebug();
    $r_right = $this->_bv[$bn]['query']->queryStmt();
    if($r_right === false){
      $this->_bv[$bn]['err_msg'] .= ' ' . $this->_bv[$bn]['query']->getLastPdoErrorMessage();
    }
    //\hat\dbg::alert($r_right);

    //update inserting node
    $this->_bv[$bn]['query']->reset();
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->update($this->_bv[$bn]['model_name'])
            ->set($this->_bv[$bn]['lft_key'], $lft)
            ->set($this->_bv[$bn]['rgt_key'], $lft + 1)
            ->set($this->_bv[$bn]['root_id_key'], $this->_bv[$bn]['dest_root_id'])
            ->set($this->_bv[$bn]['level_key'], $this->_bv[$bn]['dest_level'] + 1)
            ->where("id =?", $this->_bv[$bn]['id']);
    //$r_insert = $this->_bv[$bn]['query']->queryDebug();
    $r_insert = $this->_bv[$bn]['query']->queryStmt();
//      \hat\dbg::alert($this->_bv[$bn]['level_key']);
//      \hat\dbg::alert($this->_bv[$bn]['dest_level'] + 1);
//      \hat\dbg::alert($r_insert);
    if($r_insert === false){
      $this->_bv[$bn]['err_msg'] .= ' ' . $this->_bv[$bn]['query']->getLastPdoErrorMessage();
    }else{
      $new_result = array(
          $this->_bv[$bn]['lft_key'] => $lft,
          $this->_bv[$bn]['rgt_key'] => $lft + 1,
          $this->_bv[$bn]['root_id_key'] => $this->_bv[$bn]['dest_root_id'],
          $this->_bv[$bn]['level_key'] => $this->_bv[$bn]['dest_level'] + 1,
      );
      $this->_table->refreshResults($new_result);
    }

    if(empty($this->_bv[$bn]['err_msg'])){
//        \hat\dbg::alert('insert ok');
      $this->_bv[$bn]['query']->commit();
    }else{
//      \hat\dbg::alert('ERROR : ' . $this->_bv[$bn]['err_msg']);
      throw new \Exception("Error inserting node to tree: {$this->_bv[$bn]['err_msg']}.");
      $this->_bv[$bn]['query']->rollback();
      return false;
    }

    return true;
  }

  /**
   *
   * @param Table $destination
   */
  private function _moveNode($left_border, $right_border, $new_lft){

    $bn = 'nestedset';
    if($this->_bv[$bn]['dest_lft'] > $this->_bv[$bn]['lft'] && $this->_bv[$bn]['dest_lft'] < $this->_bv[$bn]['rgt']){
      throw new \Exception("Can not move node under itself.");
      return false;
    }
    // dql_query for move shift
    $this->_bv[$bn]['query']->begin();

    // set root_id to 0 so we could map this sub tree later
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->update($this->_bv[$bn]['model_name'])
            ->set($this->_bv[$bn]['root_id_key'], -1)
            ->where("{$this->_bv[$bn]['lft_key']} >=?", $this->_bv[$bn]['lft'])
            ->andWhere("{$this->_bv[$bn]['rgt_key']} <=?", $this->_bv[$bn]['rgt'])
            ->andWhere("{$this->_bv[$bn]['root_id_key']} = ?", $this->_bv[$bn]['root_id']);
    //$r_ = $this->_bv[$bn]['query']->queryDebug();
    $r_ = $this->_bv[$bn]['query']->queryStmt();
    if($r_ === false){
      throw new \Exception("Error inserting node to tree: {$this->_bv[$bn]['query']->getLastPdoErrorMessage()}.");
      $this->_bv[$bn]['query']->rollback();
      return false;
    }

    $this->_bv[$bn]['query']->reset();

    $r = $this->_shiftNestedRange($left_border, $right_border);
    if($r == false){
      return false;
    }

    $this->_bv[$bn]['query']->reset();

    $new_level = $this->_bv[$bn]['dest_level'] + 1;
    $reorder_index = $new_lft - $this->_bv[$bn]['lft'];
    $reorder_level_index = $new_level - $this->_bv[$bn]['level'];
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->update($this->_bv[$bn]['model_name'])
            ->set($this->_bv[$bn]['root_id_key'], $this->_bv[$bn]['dest_root_id'])
            ->set("{$this->_bv[$bn]['lft_key']} = {$this->_bv[$bn]['lft_key']} + ?", $reorder_index)
            ->set("{$this->_bv[$bn]['rgt_key']} = {$this->_bv[$bn]['rgt_key']} + ?", $reorder_index)
            ->set("{$this->_bv[$bn]['level_key']} = {$this->_bv[$bn]['level_key']} + ?", $reorder_level_index)
            ->where("{$this->_bv[$bn]['lft_key']} >=?", $this->_bv[$bn]['lft'])
            ->andWhere("{$this->_bv[$bn]['rgt_key']} <=?", $this->_bv[$bn]['rgt'])
            ->andWhere("{$this->_bv[$bn]['root_id_key']} = ?", -1);
    //$r_ = $this->_bv[$bn]['query']->queryDebug(); \hat\dbg::alert($r_);
    $r_ = $this->_bv[$bn]['query']->queryStmt();
    if($r_ === false){
      throw new \Exception("Error inserting node to tree: {$this->_bv[$bn]['query']->getLastPdoErrorMessage()}.");
      $this->_bv[$bn]['query']->rollback();
      return false;
    }else{
      if($r_ == 0){
        throw new \Exception("0 updated (3)");
        $this->_bv[$bn]['query']->rollback();
        return false;
      }
      $new_result = array(
          $this->_bv[$bn]['lft_key'] => $this->_bv[$bn]['lft'] + $reorder_index,
          $this->_bv[$bn]['rgt_key'] => $this->_bv[$bn]['rgt'] + $reorder_index,
          $this->_bv[$bn]['level_key'] => $this->_bv[$bn]['level'] + $reorder_level_index,
      );
      //\hat\dbg::alert($new_result);
      $this->_table->refreshResults($new_result);
    }

    if(empty($this->_bv[$bn]['err_msg'])){
      //\hat\dbg::alert($r_);
      $this->_bv[$bn]['query']->commit();
    }else{
//      \hat\dbg::alert('ERROR : ' . $this->_bv[$bn]['err_msg']);
      throw new \Exception("Error inserting node to tree: {$this->_bv[$bn]['err_msg']}.");
      $this->_bv[$bn]['query']->rollback();
      return false;
    }
    return true;
  }
  public function moveAsFirstChildOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      // ok
    }else{
      throw new \Exception('Destination must be a tree.');
      return false;
    }
    if($this->_table->isTree() && $this->_table->isInTree()){
      // ok
    }else{
      throw new \Exception('Node must be in a tree.');
      return false;
    }

    $bn = 'nestedset';
    $this->_extract_nestedset_vars($destination);

    if($this->_bv[$bn]['move_to_the_left']){
      // for move to the left
      $new_dest_lft = $this->_bv[$bn]['dest_lft'];
      $new_lft = $new_dest_lft + 1;
      $left_border = $this->_bv[$bn]['dest_lft'];
      $right_border = $this->_bv[$bn]['lft'];
    }else{
      // for move to the right
      $this->_bv[$bn]['delta'] *= -1;
      $new_dest_lft = $this->_bv[$bn]['dest_lft'] + $this->_bv[$bn]['delta'];
      $new_lft = $new_dest_lft + 1;
      $left_border = $this->_bv[$bn]['rgt'];
      $right_border = $this->_bv[$bn]['dest_lft'] + 1;
    }
//    $this->_bv[$bn]['right_border'] = $right_border;
//    $this->_bv[$bn]['left_border'] = $left_border;
//    $this->_bv[$bn]['new_lft'] = $new_lft;
    return $this->_moveNode($left_border, $right_border, $new_lft);
  }

  public function moveAsLastChildOf($destination){
    if($destination->isTree() && $destination->isInTree()){
      // ok
    }else{
      throw new \Exception('Destination must be a tree.');
      return false;
    }
    if($this->_table->isTree() && $this->_table->isInTree()){
      // ok
    }else{
      throw new \Exception('Node must be in a tree.');
      return false;
    }

    $bn = 'nestedset';
    $this->_extract_nestedset_vars($destination);

    if($this->_bv[$bn]['move_to_the_left']){
      // for move to the left
      $new_lft = $this->_bv[$bn]['dest_rgt'];
      $left_border = $this->_bv[$bn]['dest_rgt'] - 1;
      $right_border = $this->_bv[$bn]['lft'];
    }else{
      // for move to the right
      $this->_bv[$bn]['delta'] *= -1;
      $new_lft = $this->_bv[$bn]['dest_rgt'] + $this->_bv[$bn]['delta'];
      $left_border = $this->_bv[$bn]['rgt'];
      $right_border = $this->_bv[$bn]['dest_rgt'];
    }
    return $this->_moveNode($left_border, $right_border, $new_lft);
  }

  public function moveBefore($destination){
    if($destination->isTree() && $destination->isInTree()){
      // ok
    }else{
      throw new \Exception('Destination must be a tree.');
      return false;
    }
    if($this->_table->isTree() && $this->_table->isInTree()){
      // ok
    }else{
      throw new \Exception('Node must be in a tree.');
      return false;
    }

    $bn = 'nestedset';
    $this->_extract_nestedset_vars($destination);

    if($this->_bv[$bn]['move_to_the_left']){
      // for move to the left
      $new_lft = $this->_bv[$bn]['dest_lft'];
      $left_border = $this->_bv[$bn]['dest_lft'] - 1;
      $right_border = $this->_bv[$bn]['lft'];
    }else{
      // for move to the right
      $this->_bv[$bn]['delta'] *= -1;
      $new_lft = $this->_bv[$bn]['dest_lft'] + $this->_bv[$bn]['delta'];
      $left_border = $this->_bv[$bn]['rgt'];
      $right_border = $this->_bv[$bn]['dest_lft'];
    }
    return $this->_moveNode($left_border, $right_border, $new_lft);
  }
  public function moveAfter($destination){
    if($destination->isTree() && $destination->isInTree()){
      // ok
    }else{
      throw new \Exception('Destination must be a tree.');
      return false;
    }
    if($this->_table->isTree() && $this->_table->isInTree()){
      // ok
    }else{
      throw new \Exception('Node must be in a tree.');
      return false;
    }

    $bn = 'nestedset';
    $this->_extract_nestedset_vars($destination);

    if($this->_bv[$bn]['move_to_the_left']){
      // for move to the left
      $new_lft = $this->_bv[$bn]['dest_rgt'] + 1;
      $left_border = $this->_bv[$bn]['dest_rgt'];
      $right_border = $this->_bv[$bn]['lft'];
    }else{
      // for move to the right
      $this->_bv[$bn]['delta'] *= -1;
      $new_dest_rgt = $this->_bv[$bn]['dest_rgt'] + $this->_bv[$bn]['delta'];
      $new_lft = $new_dest_rgt + 1;
      $left_border = $this->_bv[$bn]['rgt'];
      $right_border = $this->_bv[$bn]['dest_rgt'] + 1;
    }

    return $this->_moveNode($left_border, $right_border, $new_lft);
  }

  private function _shiftNestedRange($left_border, $right_border){
    // shift lft
    $bn = 'nestedset';
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->update($this->_bv[$bn]['model_name'])
            ->set("{$this->_bv[$bn]['lft_key']} = {$this->_bv[$bn]['lft_key']} + ?", $this->_bv[$bn]['delta'])
            ->where("{$this->_bv[$bn]['lft_key']} >?", $left_border)
            ->andWhere("{$this->_bv[$bn]['lft_key']} <?", $right_border)
            ->andWhere("{$this->_bv[$bn]['root_id_key']} = ?", $this->_bv[$bn]['root_id']);
    //$r_left = $this->_bv[$bn]['query']->queryDebug();
    $r_left = $this->_bv[$bn]['query']->queryStmt();
    if($r_left === false){
      throw new \Exception("Error inserting node to tree: {$this->_bv[$bn]['query']->getLastPdoErrorMessage()}.");
      $this->_bv[$bn]['query']->rollback();
      return false;
    }elseif($r_left == 0){
      //\hat\dbg::alert($this->_bv[$bn]['query']->queryDebug());
      throw new \Exception("0 updated (1)");
      $this->_bv[$bn]['query']->rollback();
      return false;
    }
    // shift rgt
    $this->_bv[$bn]['query']->reset();
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->update($this->_bv[$bn]['model_name'])
            ->set("{$this->_bv[$bn]['rgt_key']} = {$this->_bv[$bn]['rgt_key']} + ?", $this->_bv[$bn]['delta'])
            ->where("{$this->_bv[$bn]['rgt_key']} >?", $left_border)
            ->andWhere("{$this->_bv[$bn]['rgt_key']} <?", $right_border)
            ->andWhere("{$this->_bv[$bn]['root_id_key']} = ?", $this->_bv[$bn]['root_id']);
    //$r_left = $this->_bv[$bn]['query']->queryDebug();
    $r_left = $this->_bv[$bn]['query']->queryStmt();
    if($r_left === false){
      throw new \Exception("Error inserting node to tree: {$this->_bv[$bn]['query']->getLastPdoErrorMessage()}.");
      $this->_bv[$bn]['query']->rollback();
      return false;
    }elseif($r_left == 0){
      \hat\dbg::alert($this->_bv[$bn]['query']->queryDebug());
      throw new \Exception("0 updated (2)");
      $this->_bv[$bn]['query']->rollback();
      return false;
    }

    //\hat\dbg::alert($r_left);
    //\hat\dbg::alert($r_right);
    return true;

  }

  public function isLeaf(){
    if($this->_table->isTree() && $this->_table->isInTree()){
      // ok
    }else{
      throw new \Exception('Model must be a tree.');
      return false;
    }
    $bn = 'nestedset';
    $this->_extract_nestedset_vars();
    if($this->_bv[$bn]['delta'] == 2){
      return true;
    }
    return false;
  }
  public function isRoot(){
    if($this->_table->isTree() && $this->_table->isInTree()){
      // ok
    }else{
      throw new \Exception('Model must be a tree.');
      return false;
    }
    $bn = 'nestedset';
    $this->_extract_nestedset_vars();
    if($this->_bv[$bn]['lft'] == 1){
      return true;
    }
    return false;
  }
  public function getRoots(){
    if($this->_table->isTree()){
      // ok
    }else{
      throw new \Exception('Model must be a tree.');
      return false;
    }
    $bn = 'nestedset';
    $this->_extract_nestedset_vars();
    $path_field = $this->getPathField();

    $this->_bv[$bn]['query']->reset();
    $this->_bv[$bn]['query'] = $this->_bv[$bn]['query']->select("_tt.id, _tt.$path_field")
            ->from($this->_bv[$bn]['model_name'] . ' _tt')
            ->where("{$this->_bv[$bn]['level_key']} =?", 0)
            ->andWhere("{$this->_bv[$bn]['root_id_key']} >?", 0);
    //$r_roots = $this->_bv[$bn]['query']->queryDebug();
    $r_roots = $this->_bv[$bn]['query']->queryStmt();
    if($r_roots === false){
      throw new \Exception("Error getting roots: {$this->_bv[$bn]['query']->getLastPdoErrorMessage()}.");
      return false;
    }elseif($r_roots == 0){
//      \hat\dbg::alert($this->_bv[$bn]['query']->queryDebug());
      return array();
    }

    //$r = $this->_bv[$bn]['query']->getResultsDbRow();
    $return = array();
    $roots = $this->_bv[$bn]['query']->getResults();
    foreach($roots as $root){
      $return[$root['id']] = $root[$path_field];
    }
    //\hat\dbg::alert($r, true);
    return $return;

  }


//  private function  __init_nestedset($options) {
//  }
  private function __pre_save_nestedset($options){
    $this->_extract_nestedset_vars();
    $keys = array();
    $keys[] = $this->_bv['nestedset']['lft_key'];
    $keys[] = $this->_bv['nestedset']['rgt_key'];
    $keys[] = $this->_bv['nestedset']['root_id_key'];
    $keys[] = $this->_bv['nestedset']['level_key'];
    $this->_table->unsetForSave($keys);
  }
  private function __pre_delete_nestedset($options){
    if($this->_table->isInTree()){
      $this->_extract_nestedset_vars();
      $this->_bv['nestedset']['query']->begin();
      $this->removeFromTree();
    }
  }
  private function __post_delete_nestedset($options){
    if(isset($this->_bv['nestedset'])){
      if(empty($this->_bv['nestedset']['err_msg'])){
        $this->_bv['nestedset']['query']->commit();
      }else{
        $this->_bv['nestedset']['query']->rollback();
      }
    }
  }


  /*
   * multitenant
   */
  private function __init_multitenant($options){
    $b_name = 'multitenant';
    if(!isset($this->_used_behaviors[$b_name]['identifier'])){
      throw new \Exception("Could not load Multitenant behavior without multitenant identifier.");
    }

  }
  private function __DalSelect_multitenant($options){
    $alias = $this->_table->getAlias();
    //\hat\dbg::alert('in DalSelect hook :: ' . $alias);
    $field = "$alias.{$options['fields']['identifier'][0]} =?";
    //\hat\dbg::alert(array('field' => $field, 'contains' => $this->_query->contains($field)));
    //\hat\dbg::alert(compact('alias', 'field'));
    if( ! $this->_query->contains($field)){
      $this->_query->andWhere($field, $options['identifier']);
    }
    $this->_query->addSelect("$alias.{$options['fields']['identifier'][0]}");
  }
  private function __DalJoin_multitenant($options){
    $alias = $this->_table->getAlias();
    //\hat\dbg::alert('in DalSelect hook :: ' . $alias);
    $field = "$alias.{$options['fields']['identifier'][0]} =?";
    //\hat\dbg::alert(array('field' => $field, 'contains' => $this->_query->contains($field)));
    $table_info = $this->_query->getTableInfoByAlias($alias);
    if($table_info && isset($table_info['join_index'])){
      $this->_query->setJoinCondition(array($field => $options['identifier']), $table_info['join_index']);
    }else{
      return false;
    }
    //\hat\dbg::alert(compact('alias', 'field', 'table_info'));
//    if( ! $this->_query->contains($field)){
//      $this->_query->andWhere($field, $options['identifier']);
//    }
//    $this->_query->addSelect("$alias.{$options['fields']['identifier'][0]}");

  }
  private function __DalDelete_multitenant($options){
    //$alias = $this->_table->getAlias();
    //\hat\dbg::alert('in DalDelete hook :: ' . $alias);
    //$field = "$alias.{$options['fields']['identifier'][0]} =?";
    $field = "{$options['fields']['identifier'][0]} =?";
    //\hat\dbg::alert(array('field' => $field, 'contains' => $this->_query->contains($field)));
    if( ! $this->_query->contains($field)){
      $this->_query->andWhere($field, $options['identifier']);
    }
    //$this->_query->addSelect("$alias.{$options['fields']['identifier'][0]}");
  }
  private function __DalInsert_multitenant($options){
    //$alias = $this->_table->getAlias();
    //\hat\dbg::alert('in DalInsert hook :: ' . $alias);
    //$field = "$alias.{$options['fields']['identifier'][0]} =?";
    $field = $options['fields']['identifier'][0];
    //\hat\dbg::alert(array('field' => $field, 'contains' => $this->_query->contains($field)));
    if( ! $this->_query->contains($field)){
      $this->_query->value($field, $options['identifier']);
//      \hat\dbg::alert("in DalInsert hook :: $field :: {$options['identifier']}");
    }
    //$this->_query->addSelect("$alias.{$options['fields']['identifier'][0]}");

  }
  private function __DalUpdate_multitenant($options){
    // check in where
    $field = "{$options['fields']['identifier'][0]} =?";
    if( ! $this->_query->contains($field)){
      $this->_query->andWhere($field, $options['identifier']);
//      \hat\dbg::alert("in DalUpdate hook :: $field :: {$options['identifier']}");
    }

    // check in set
    $field = $options['fields']['identifier'][0];
    //\hat\dbg::alert(array('field' => $field, 'contains' => $this->_query->contains($field)));
    if($this->_query->contains($field)){
      if($options['allow_tenant_update']){
        // ok, tenant update is allowed
      }else{
        throw new \Exception('Tenant identifier update not allowed.');
      }
      //$this->_query->set($field, $options['identifier']);
    }
//      \hat\dbg::alert("in DalUpdate hook :: $field :: {$options['identifier']}");
    //$this->_query->addSelect("$alias.{$options['fields']['identifier'][0]}");
  }


  private function __pre_save_multitenant($options){
    if( !$this->_table->isNew() || $this->_table->isLoaded()){
      $field = $options['fields']['identifier'][0];

      $old_identifier = $this->_table->getOldValue($field);
      if(!$old_identifier){
        return;
      }
//    \hat\dbg::alert($this->_table->toArray());
      \hat\dbg::alert($old_identifier);
      if($old_identifier != $options['identifier']){
        throw new \Exception('Trying to modify item from another tenant.');
      }


      $new_identifier = $this->_table->getForSave($field);
      //\hat\dbg::alert("new_identifier = $new_identifier");
      if($new_identifier){
        throw new \Exception('Trying to modify tenant identifier (by changing tenant identification).');
      }

    }
//    \hat\dbg::alert("__pre_save_multitenant__");
//    \hat\dbg::alert($this->_table->toArray());
//    \hat\dbg::alert($this->_table->isNew());
//    \hat\dbg::alert($this->_table->isLoaded());
  }

  /*
   * sluggable
   */

  /*
   * softdelete
   */
  private function __DalSelect_softdelete($options){
    $alias = $this->_table->getAlias();
    //\hat\dbg::alert('in DalSelect hook :: ' . $alias);
    $field = "$alias.{$options['fields']['deleted_at'][0]} IS NULL ";
    //\hat\dbg::alert(array('field' => $field, 'contains' => $this->_query->contains($field)));
    //\hat\dbg::alert(compact('alias', 'field'));
    if( ! $this->_query->contains($field)){
      $this->_query->andWhere($field, null);
    }
    //$this->_query->addSelect("$alias.{$options['fields']['identifier'][0]}");
  }
  private function __DalDelete_softdelete($options){
    return;
    //$alias = $this->_table->getAlias();
    //\hat\dbg::alert('in DalDelete hook :: ' . $alias);
    //$field = "$alias.{$options['fields']['identifier'][0]} =?";
    $field = "{$options['fields']['identifier'][0]} =?";
    //\hat\dbg::alert(array('field' => $field, 'contains' => $this->_query->contains($field)));
    if( ! $this->_query->contains($field)){
      $this->_query->andWhere($field, $options['identifier']);
    }
    //$this->_query->addSelect("$alias.{$options['fields']['identifier'][0]}");
  }
  private function __pre_delete_softdelete($options){
    $bn = 'softdelete'; // behavior name
    if(!$this->_table->isNew()){
      $now = \date($options['format']);
      $this->_table->set($options['fields']['deleted_at'][0], $now);
      $this->_table->save();
      return false;
    }
  }


  /*
   * timestampable
   */
  private function __init_timestampable($options){

  }
  private function __pre_save_timestampable($options){
    $now = \date($options['format']);
//        \hat\dbg::alert($now);
//        \hat\dbg::alert($options['fields']['created_at'][0]);
    if($this->_table->isNew()){
      $this->_table->set($options['fields']['created_at'][0], $now);
    }
    $this->_table->set($options['fields']['updated_at'][0], $now);
    //
  }
  private function __post_save_timestampable($options){
    //
  }

  /*
   * blameable
   */
  private function __init_blameable($options){
  }
  private function __pre_save_blameable($options){
    $who = $this->_blameable_get_who_to_blame($options);
    if($this->_table->isNew()){
      $this->_table->set($options['fields']['created_by'][0], $who);
    }
    $this->_table->set($options['fields']['updated_by'][0], $who);
    //
  }

  private function __post_save_blameable($options){
    //
  }
  private function _blameable_get_who_to_blame($options){
    $who_to_blame = isset($_SESSION[$options['blame_var']]) ? $_SESSION[$options['blame_var']] : null;
    if(!isset($who_to_blame)){
      $who_to_blame = $options['default'];
    }
    return $who_to_blame;
  }

}
?>