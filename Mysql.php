<?php
namespace hatwebtech\dal;
/**
 * Mysql specific query parts
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


 */
class Mysql extends DalQuery {
  protected function _parseQueryParts_limit_offset(){
//    $limit = ' --- MySql limit sql --- ';
//    $this->_sql_query_parts['limit'][] = $limit;
//    return true;

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
}
?>