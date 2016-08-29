/*
 * lib-sql - Java helper library for building SQL commands
 * Copyright (C) 2016  Fabio Seiiti Sannomiya <dev@seiiti.com>
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General 
 * Public License along with this program.  If not, see 
 * <http://www.gnu.org/licenses/>.
 */
package com.seiiti.sql;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.seiiti.expression.booleanoperation.Between;
import com.seiiti.expression.booleanoperation.BooleanOperation;
import com.seiiti.expression.booleanoperation.Equals;
import com.seiiti.expression.booleanoperation.GreaterThan;
import com.seiiti.expression.booleanoperation.In;
import com.seiiti.expression.booleanoperation.LessThan;
import com.seiiti.expression.booleanoperation.Like;

public class SqlWhereBuilder {
  private ArrayList<String> where = new ArrayList<String>();
  private ArrayList<Object> values = new ArrayList<Object>();
  
  public SqlWhereBuilder() {
  }
  
  public SqlWhereBuilder(List<BooleanOperation> ops) {
    for(BooleanOperation op : ops)
      add(op);
  }
  
  public void eq(String columnName, Object value) {
    final boolean neg = false;
    add(new Equals(columnName, value, neg));
  }
  
  public void neq(String columnName, Object value) {
    final boolean neg = true;
    add(new Equals(columnName, value, neg));
  }
  
  private void add(Equals op) {
    if(op.value != null) {
      String operator = op.neg ? "<>" : "=";
      
      this.where.add(op.columnName + " " + operator + " ?");
      this.values.add(op.value);
    }
    else if(op.neg)
      this.where.add(op.columnName + " IS NOT NULL");
    else
      this.where.add(op.columnName + " IS NULL");
  }
  
  public void lt(String columnName, Object value) {
    add(new LessThan(columnName, value));
  }
  
  private void add(LessThan op) {
    this.where.add(op.columnName + " < ?");
    this.values.add(op.value);
  }
  
  public void gt(String columnName, Object value) {
    add(new GreaterThan(columnName, value));
  }
  
  private void add(GreaterThan op) {
    this.where.add(op.columnName + " > ?");
    this.values.add(op.value);
  }
  
  public void between(String columnName, Object start, Object end) {
    add(new Between(columnName, start, end));
  }
  
  private void add(Between op) {
    if(op.lower != null)
      if(op.upper != null) {
        this.where.add(op.columnName + " BETWEEN ? AND ?");
        this.values.add(op.lower);
        this.values.add(op.upper);
      }
      else {
        this.where.add(op.columnName + " >= ?");
        this.values.add(op.lower);
      }
    else if(op.upper != null) {
      this.where.add(op.columnName + " <= ?");
      this.values.add(op.upper);
    }
  }
  
  public <T> void in(String columnName, T[] values) {
    final boolean notIn = false;
    add(new In(columnName, values != null ? Arrays.asList(values) : null, notIn));
  }
  
  public <T> void in(String columnName, List<T> values) {
    boolean notIn = false;
    add(new In(columnName, values, notIn));
  }
  
  public <T> void nin(String columnName, T[] values) {
    boolean neg = true;
    add(new In(columnName, values != null ? Arrays.asList(values) : null, neg));
  }
  
  public <T> void nin(String columnName, List<T> values) {
    boolean neg = true;
    add(new In(columnName, values, neg));
  }
  
  private void add(In op) {
    if(op.values == null || op.values.size() == 0)
      return;
    
    String operator = op.neg ? "NOT IN" : "IN";
    
    String[] qm; {
      qm = new String[op.values.size()];
      Arrays.fill(qm, "?");
    }
    
    this.where.add(
      String.format("%s %s (%s)", op.columnName, operator, stream(qm).collect(joining(", ")))
    );
    
    this.values.addAll(op.values);
  }
  
  public <T> void like(String columnName, String value) {
    add(new Like(columnName, value));
  }
  
  private void add(Like op) {
    this.where.add(
      String.format("%s LIKE ?", op.columnName)
    );
    
    this.values.add(String.format("%%%s%%", op.value));
  }
  
  public void add(BooleanOperation op) {
    if(op instanceof Equals)
      add((Equals)op);
    else if(op instanceof LessThan)
      add((LessThan)op);
    else if(op instanceof GreaterThan)
      add((GreaterThan)op);
    else if(op instanceof Between)
      add((Between)op);
    else if(op instanceof In)
      add((In)op);
    else if(op instanceof Like)
      add((Like)op);
    else
      throw new UnsupportedOperationException();
  }
  
  private Object filterValue(Object value) {
    // converte java.util.Date para java.sql.Date
    if(value != null && "java.util.Date".equals(value.getClass().getName()))
      return new java.sql.Date(((java.util.Date)value).getTime());
    
    return value;
  }
  
  /**
   * <p>
   * Devolve a string
   * <pre>
   * WHERE [condição]</pre>
   * se houverem condições setadas.
   * Caso contrário devolve string vazia.</p>
   * <p>
   * Obs: antes do {@code WHERE} existe um espaço em branco e após {@code [condição]}, não.
   * A intenção é que o resultado deste método seja utilizado em algo como:
   * <pre>
   * String.format(
   *   "SELECT c FROM tabela%s ORDER BY x",
   *   sqlWhereBuilder.getWhereClause()
   * )</pre>
   * Note a ausência do espaço entre {@code tabela} e {@code %s} na string de formatação.</p>
   * @return
   */
  public String getWhere() {
    if(this.where.size() == 0)
      return "";
    
    return String.format(" WHERE %s", getWhereExpression());
  }
  
  /**
   * Devolve somente a expressão da cláusula where, sem preceder pela palavra {@code WHERE}.
   * @return
   */
  public String getWhereExpression() {
    return this.where.stream().collect(joining(" AND "));
  }
  
  public Object[] getValues() {
    int count = this.values.size();
    Object[] values = new Object[count];
    
    for(int i = 0; i < count; i++) {
      Object value = this.values.get(i);
      values[i] = filterValue(value);
    }
    
    return values;
  }
}
