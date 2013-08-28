DROP TABLE IF EXISTS go_const_names;

SELECT
  main.oid,
  'T_' || main.typname as const,
  'T_' || main.typname || ' Oid = ' || main.oid as assignment,
  arr.oid as array_oid,
  'T_' || arr.typname as array_const,
  elem.oid as elem_oid,
  'T_' || elem.typname as elem_const,
  main.typcategory
INTO TEMP TABLE go_const_names
FROM pg_type as main
  LEFT JOIN pg_type as arr
    ON arr.oid = main.typarray
  LEFT JOIN pg_type as elem
    ON elem.oid = main.typelem
WHERE main.oid < 10000
ORDER BY main.oid;

SELECT assignment FROM go_const_names;

SELECT 'ArrayType[' || main.const || '] = ' || arr.const
FROM go_const_names as main
  INNER JOIN go_const_names as arr
    ON main.array_oid = arr.oid;

SELECT 'ElementType[' || main.const || '] = ' || elem.const
FROM go_const_names as main
  INNER JOIN go_const_names as elem
    ON main.elem_oid = elem.oid;

SELECT 'category[' || main.const || '] = ''' || typcategory || ''''
FROM go_const_names AS main;
