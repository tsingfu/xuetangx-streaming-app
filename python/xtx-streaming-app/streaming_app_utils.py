# -*- coding: utf-8 -*-


def map_conf_properties(obj, key_name=None):
    properties_dict = obj.get('properties')
    # print(type(properties_dict))
    if properties_dict is not None and isinstance(properties_dict, dict):
        for k, v in properties_dict.items():
            obj[k] = v
        del (obj['properties'])
    # print('= = ' * 20, '[debug map_conf_properties]', type(obj), 'obj = ')
    if key_name is None:
        res = obj
    else:
        if obj.has_key(key_name):
            res = {obj.get(key_name): obj}
        else:
            raise Exception('[myapp Exception] not found keyName in dict')
    # pprint(res)
    return res


# def dict_merge(dict1, dict2, update_flag=False, merge_key_name=None):
#     if dict1 is None or not isinstance(dict1, dict):
#         dict1 = {}
#
#     if dict2 is not None and isinstance(dict2, dict):
#         for k, v in dict2.items():
#             if update_flag or not dict1.has_key(k):
#                 dict1[k] = v
#
#     # print('= = ' * 20, '[debug dict_merge]', type(dict1), 'dict1 = ')
#     if merge_key_name is None:
#         res = dict1
#     else:
#         res = {dict1.get(merge_key_name): dict1}
#     # pprint(res)
#     return res


def dict_merge(dict1, dict2, update_flag=False, merge_key_name=None):
    """
    合并字典
    构建新字典的原因，spark的操作是lazy，如果使用可变的字典，可能遇到的问题：在触发 job 执行时，配置被覆盖
    """
    ret = {}
    if isinstance(dict1, dict):
        for k, v in dict1.iteritems():
            ret[k]=v

    if isinstance(dict2, dict):
        for k, v in dict2.items():
            if update_flag or not ret.has_key(k):
                ret[k] = v

    # print('= = ' * 20, '[debug dict_merge]', type(dict1), 'dict1 = ')
    if merge_key_name is not None:
        ret = {ret.get(merge_key_name): ret}
    # pprint(ret)
    return ret


def list_dict_merge(list1):
    # print('= = ' * 20, '[myapp debug list_dict_merge] list1 = ', )
    # pprint(list1)
    res = reduce(dict_merge, [{}] if len(list1) == 0 else list1)
    return res


def get_di_conf_with_ds_conf(di_id, di_confs, ds_confs, di_key, di_ds_key, ds_key, merge_key_name=None):
    di_conf = map_conf_properties(di_confs[di_id], key_name=di_key)
    ds_id = di_conf[di_id].get(di_ds_key)
    if ds_id is not None:
        ds_conf = map_conf_properties(ds_confs.get(ds_id), key_name=ds_key).get(ds_id)
        res = dict_merge(di_conf.get(di_id), ds_conf, merge_key_name=merge_key_name)
    else:
        res = di_conf
    return res


def get_class_obj(class_path):
    module_name = '.'.join(class_path.split('.')[0:-1])
    class_name = class_path.split('.')[-1]
    # print(module_name, class_name)
    module_obj = __import__(module_name)
    # print(type(module_obj), 'module_obj = ', module_obj)
    class_obj = getattr(module_obj, class_name)
    # print(type(class_obj), 'class_obj = ', class_obj)
    return class_obj


def date_string_utc2cst(date_string):
    from datetime import datetime, timedelta

    if len(date_string) < 19:
        # print('= = ' * 20, '[myapp] len(date_string) = ', len(date_string))
        new_date_string = ''
    else:  # len(date_string) >= 19:
        if date_string[10] == 'T':
            new_date = datetime.strptime(date_string[:19], '%Y-%m-%dT%H:%M:%S') + timedelta(hours=8)
        else:
            new_date = datetime.strptime(date_string[:19], '%Y-%m-%d %H:%M:%S') + timedelta(hours=8)
        new_date_string = new_date.strftime('%Y-%m-%d %H:%M:%S')

    return new_date_string


def fun_print_in_rdd_foreach(line):
    print('[myapp fun_print_in_rdd_foreach] line = ', line)


def fun_print_in_rdd_map(line):
    print('[myapp fun_print_in_rdd_map] line = ', line)
    return line


def fun_foreachRDD_count(rdd):
    print('[myapp fun_foreachRDD_count]' + str(rdd.count()))


def generate_df_schmea(schema_conf_string, add_map_flag=False, reserved_field_for_add_map=None):
    if not reserved_field_for_add_map:
        reserved_field_for_add_map = []

    struct_field_list = []
    for field_type in schema_conf_string.split(','):
        field_type_list_tmp = field_type.split(':')
        (field, typ) = tuple(field_type_list_tmp[0:2]) \
            if len(field_type_list_tmp) == 2 else (field_type_list_tmp[0], 'string')
        field_name = field.strip()
        if field_name in reserved_field_for_add_map:
            if add_map_flag:
                raise Exception('[myapp generate_df_schmea] invalid parameter with schema_conf_string, '
                                'for add_map_flag is enabled with reserved_field_for_add_map '
                                + ','.join(reserved_field_for_add_map))
            else:  # 生成 schema 时没有要求添加map字段，但字段名和 reserved_field_name 相同，约定 该字段的数据类型是 MapType
                struct_field_list.append(generate_struct_field(field_name, typ))
        else:
            struct_field_list.append(generate_struct_field(field_name, typ))

    if add_map_flag:
        for field_name in reserved_field_for_add_map:
            struct_field_list.append(generate_struct_field(field_name, typ='map'))
    from pyspark.sql.types import StructType
    return StructType(sorted(struct_field_list, key=lambda x: x.name))


def generate_struct_field(field_name, typ='string', nullable=True):
    from pyspark.sql.types import *

    TYPE_MAP = {
        'string': StringType(),
        'integer': IntegerType(),
        'short': ShortType(),
        'float': FloatType(),
        'long': LongType(),
        'double': DoubleType(),
        'decimal': DecimalType(),
        'boolean': BooleanType(),
        'date': DateType(),
        'timestamp': TimestampType(),
        'map': MapType(StringType(), StringType(), nullable)
    }

    return StructField(field_name, TYPE_MAP.get(typ), nullable)


def update_df_schema(p_schema, remove_key_list, add_key_list, deduplicate_flag=True):
    """
    更新schema结构
    """
    if len(remove_key_list) == 0 and add_key_list == 0:
        return p_schema
    else:
        from pyspark.sql.types import StructType
        # 支持配置新增、删除字段构建 schema
        struct_field_list = p_schema.fields

        # TODO: 支持添加复合结构的字段
        remove_key_set = set([x.strip() for x in remove_key_list if x.strip() != ''])

        if len(remove_key_set) != 0:
            struct_field_list2 = [x for x in struct_field_list if x.name not in remove_key_set]
        else:
            struct_field_list2 = struct_field_list

        if deduplicate_flag:
            struct_field_exists_set = set([field.name for field in struct_field_list2])
            add_key_list2 = [field_type for field_type in add_key_list
                             if field_type.split(':')[0].strip() not in struct_field_exists_set]
        else:
            add_key_list2 = add_key_list

        for field_type in add_key_list2:
            field_type_list_tmp = field_type.split(':')
            (field, typ) = tuple(field_type_list_tmp[0:2]) \
                if len(field_type_list_tmp) == 2 else (field_type_list_tmp[0], 'string')
            field_name = field.strip()

            struct_field_list2.append(generate_struct_field(field_name, typ))

        schema_new = StructType(sorted(struct_field_list2, key=lambda x: x.name))

        return schema_new
