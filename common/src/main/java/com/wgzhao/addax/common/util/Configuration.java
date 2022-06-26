/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.common.util;

import com.alibaba.fastjson2.JSONWriter;
import com.wgzhao.addax.common.exception.CommonErrorCode;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.spi.ErrorCode;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Configuration 提供多级JSON配置信息无损存储 <br>
 * 实例代码:<br>
 * 获取job的配置信息<br>
 * Configuration configuration = Configuration.from(new File("Config.json")); <br>
 * String jobContainerClass =
 * configuration.getString("core.container.job.class"); <br>
 * 设置多级List <br>
 * configuration.set("job.reader.parameter.jdbcUrl", Arrays.asList(new String[]
 * {"jdbc", "jdbc"}));
 * 合并Configuration: <br>
 * configuration.merge(another);
 * Configuration 存在两种较好地实现方式<br>
 * 第一种是将JSON配置信息中所有的Key全部打平，用a.b.c的级联方式作为Map的Key，内部使用一个Map保存信息 <br>
 * 第二种是将JSON的对象直接使用结构化树形结构保存<br>
 * 目前使用的第二种实现方式，使用第一种的问题在于: <br>
 * 1. 插入新对象，比较难处理，例如a.b.c="foo"，此时如果需要插入a="foo"，也即是根目录下第一层所有类型全部要废弃
 * ，使用"foo"作为value，第一种方式使用字符串表示key，难以处理这类问题。 <br>
 * 2. 返回树形结构，例如 a.b.c.d = "foo"，如果返回"a"下的所有元素，实际上是一个Map，需要合并处理 <br>
 * 3. 输出JSON，将上述对象转为JSON，要把上述Map的多级key转为树形结构，并输出为JSON <br>
 */
public class Configuration
{

    /**
     * 对于加密的keyPath，需要记录下来
     * 为的是后面分布式情况下将该值加密后抛到 AddaxServer中
     */
    private Set<String> secretKeyPathSet =
            new HashSet<>();

    private Object root;

    private Configuration(String json)
    {
        try {
            this.root = JSON.parse(json);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CommonErrorCode.CONFIG_ERROR,
                    String.format("配置信息错误. 您提供的配置信息不是合法的JSON格式: %s . 请按照标准json格式提供配置信息. ", e.getMessage()));
        }
    }

    /*
     * 初始化空白的Configuration
     */
    public static Configuration newDefault()
    {
        return Configuration.from("{}");
    }

    /*
     * 从JSON字符串加载Configuration
     */
    public static Configuration from(String json)
    {
        json = StrUtil.replaceVariable(json);
        checkJSON(json);

        try {
            return new Configuration(json);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CommonErrorCode.CONFIG_ERROR,
                    e);
        }
    }

    /*
     * 从包括json的File对象加载Configuration
     */
    public static Configuration from(File file)
    {
        try {
            return Configuration.from(IOUtils
                    .toString(new FileInputStream(file), StandardCharsets.UTF_8));
        }
        catch (FileNotFoundException e) {
            throw AddaxException.asAddaxException(CommonErrorCode.CONFIG_ERROR,
                    String.format("配置信息错误，您提供的配置文件[%s]不存在. 请检查您的配置文件.", file.getAbsolutePath()));
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.CONFIG_ERROR,
                    String.format("配置信息错误. 您提供配置文件[%s]读取失败，错误原因: %s. 请检查您的配置文件的权限设置.",
                            file.getAbsolutePath(), e));
        }
    }

    /*
     * 从包括json的InputStream对象加载Configuration
     */
    public static Configuration from(InputStream is)
    {
        try {
            return Configuration.from(IOUtils.toString(is, StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(CommonErrorCode.CONFIG_ERROR,
                    String.format("请检查您的配置文件. 您提供的配置文件读取失败，错误原因: %s. 请检查您的配置文件的权限设置.", e));
        }
    }

    /*
     * 从Map对象加载Configuration
     */
    public static Configuration from(final Map<String, Object> object)
    {
        return Configuration.from(Configuration.toJSONString(object));
    }

    /*
     * 从List对象加载Configuration
     */
    public static Configuration from(final List<Object> object)
    {
        return Configuration.from(Configuration.toJSONString(object));
    }

    private static void checkJSON(String json)
    {
        if (StringUtils.isBlank(json)) {
            throw AddaxException.asAddaxException(CommonErrorCode.CONFIG_ERROR,
                    "配置信息错误. 因为您提供的配置信息不是合法的JSON格式, JSON不能为空白. 请按照标准json格式提供配置信息. ");
        }
    }

    private static String toJSONString(Object object)
    {
        return JSON.toJSONString(object);
    }

    public String getNecessaryValue(String key, ErrorCode errorCode)
    {
        String value = this.getString(key, null);
        if (StringUtils.isBlank(value)) {
            throw AddaxException.asAddaxException(errorCode,
                    String.format("您提供配置文件有误，[%s]是必填参数，不允许为空或者留白 .", key));
        }

        return value;
    }

    public String getUnnecessaryValue(String key, String defaultValue)
    {
        String value = this.getString(key, defaultValue);
        if (StringUtils.isBlank(value)) {
            value = defaultValue;
        }
        return value;
    }

    /**
     * 根据用户提供的json path，寻址具体的对象。
     *
     * NOTE: 目前仅支持Map以及List下标寻址, 例如:
     *
     * 对于如下JSON
     * <p>
     * {"a": {"b": {"c": [0,1,2,3]}}}
     * </p>
     * config.get("") 返回整个Map <br>
     * config.get("a") 返回a下属整个Map <br>
     * config.get("a.b.c") 返回c对应的数组List <br>
     * config.get("a.b.c[0]") 返回数字0
     *
     * @param path String 要查的json路径
     * @return Java表示的JSON对象，如果path不存在或者对象不存在，均返回null。
     */
    public Object get(String path)
    {
        this.checkPath(path);
        try {
            return this.findObject(path);
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * 用户指定部分path，获取Configuration的子集
     *
     * @param path String json 路径
     * @return Configuration 配置对象， 如果path获取的路径或者对象不存在，返回null
     */
    public Configuration getConfiguration(String path)
    {
        Object object = this.get(path);
        if (null == object) {
            return null;
        }

        return Configuration.from(Configuration.toJSONString(object));
    }

    /**
     * 根据用户提供的json path，寻址String对象
     *
     * @param path String json 路径
     *
     * @return String 对象，如果path不存在或者String不存在，返回null
     */
    public String getString(String path)
    {
        Object string = this.get(path);
        if (null == string) {
            return null;
        }
        return String.valueOf(string);
    }

    /**
     * 根据用户提供的json path，寻址String对象，如果对象不存在，返回默认字符串
     *
     * @param path String json 路径
     * @param defaultValue String 如果path不存在，则返回该值
     *
     * @return String对象，如果path不存在或者String不存在，返回默认字符串
     */
    public String getString(String path, String defaultValue)
    {
        String result = this.getString(path);

        if (null == result) {
            return defaultValue;
        }

        return result;
    }

    /**
     * 根据用户提供的json path，寻址Character对象
     *
     * @param path String json 路径
     * @return Character对象，如果path不存在或者Character不存在，返回null
     */
    public Character getChar(String path)
    {
        String result = this.getString(path);
        if (null == result) {
            return null;
        }

        try {
            return CharUtils.toChar(result);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.CONFIG_ERROR,
                    String.format("任务读取配置文件出错. 因为配置文件路径[%s] 值非法，期望是字符类型: %s. 请检查您的配置并作出修改.", path,
                            e.getMessage()));
        }
    }

    /**
     * 根据用户提供的json path，寻址Boolean对象，如果对象不存在，返回默认Character对象
     *
     * @param path String json 路径
     * @param defaultValue String 如果path不存在，则返回该值
     *
     * @return Character对象，如果path不存在或者Character不存在，返回默认Character对象
     */
    public Character getChar(String path, char defaultValue)
    {
        Character result = this.getChar(path);
        if (null == result) {
            return defaultValue;
        }
        return result;
    }

    /**
     * 根据用户提供的json path，寻址Boolean对象
     *
     * @param path String json 路径
     *
     * @return Boolean对象，如果path值非true,false ，将报错.特别注意：当 path 不存在时，会返回：null.
     */
    public Boolean getBool(String path)
    {
        String result = this.getString(path);

        if (null == result) {
            return null; //NOSONAR
        }
        else if ("true".equalsIgnoreCase(result)) {
            return Boolean.TRUE;
        }
        else if ("false".equalsIgnoreCase(result)) {
            return Boolean.FALSE;
        }
        else {
            throw AddaxException.asAddaxException(CommonErrorCode.CONFIG_ERROR,
                    String.format("您提供的配置信息有误，因为从[%s]获取的值[%s]无法转换为bool类型. 请检查源表的配置并且做出相应的修改.",
                            path, result));
        }
    }

    /**
     * 根据用户提供的json path，寻址Boolean对象，如果对象不存在，返回默认Boolean对象
     *
     *
     * @param path String json 路径
     * @param defaultValue String 如果path不存在，则返回该值
     *
     * @return Boolean对象，如果path不存在或者Boolean不存在，返回默认Boolean对象
     */
    public Boolean getBool(String path, boolean defaultValue)
    {
        Boolean result = this.getBool(path);
        if (null == result) {
            return defaultValue;
        }
        return result;
    }

    /**
     * 根据用户提供的json path，寻址Integer对象
     *
     * @param path String json 路径
     *
     * @return Integer对象，如果path不存在或者Integer不存在，返回null
     */
    public Integer getInt(String path)
    {
        String result = this.getString(path);
        if (null == result) {
            return null;
        }

        try {
            return Integer.valueOf(result);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.CONFIG_ERROR,
                    String.format("任务读取配置文件出错. 配置文件路径[%s] 值非法, 期望是整数类型: %s. 请检查您的配置并作出修改.", path,
                            e.getMessage()));
        }
    }

    /**
     * 根据用户提供的json path，寻址Integer对象，如果对象不存在，返回默认Integer对象
     *
     * @param path String json 路径
     * @param defaultValue String 如果path不存在，则返回该值
     *
     * @return Integer对象，如果path不存在或者Integer不存在，返回默认Integer对象
     */
    public Integer getInt(String path, int defaultValue)
    {
        Integer object = this.getInt(path);
        if (null == object) {
            return defaultValue;
        }
        return object;
    }

    /**
     * 根据用户提供的json path，寻址Long对象
     *
     * @param path String json 路径
     *
     * @return Long对象，如果path不存在或者Long不存在，返回null
     */
    public Long getLong(String path)
    {
        String result = this.getString(path);
        if (StringUtils.isBlank(result)) {
            return null;
        }

        try {
            return Long.valueOf(result);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.CONFIG_ERROR,
                    String.format("任务读取配置文件出错. 配置文件路径[%s] 值非法, 期望是整数类型: %s. 请检查您的配置并作出修改.", path,
                            e.getMessage()));
        }
    }

    /**
     * 根据用户提供的json path，寻址Long对象，如果对象不存在，返回默认Long对象
     *
     * @param path String json 路径
     * @param defaultValue String 如果path不存在，则返回该值
     *
     * @return Long对象，如果path不存在或者Integer不存在，返回默认Long对象
     */
    public Long getLong(String path, long defaultValue)
    {
        Long result = this.getLong(path);
        if (null == result) {
            return defaultValue;
        }
        return result;
    }

    /**
     * 根据用户提供的json path，寻址Double对象
     *
     * @param path String json 路径
     *
     * @return Double对象，如果path不存在或者Double不存在，返回null
     */
    public Double getDouble(String path)
    {
        String result = this.getString(path);
        if (StringUtils.isBlank(result)) {
            return null;
        }

        try {
            return Double.valueOf(result);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.CONFIG_ERROR,
                    String.format("任务读取配置文件出错. 配置文件路径[%s] 值非法, 期望是浮点类型: %s. 请检查您的配置并作出修改.", path,
                            e.getMessage()));
        }
    }

    /**
     * 根据用户提供的json path，寻址Double对象，如果对象不存在，返回默认Double对象
     *
     * @param path String json 路径
     * @param defaultValue String 如果path不存在，则返回该值
     *
     * @return Double对象，如果path不存在或者Double不存在，返回默认Double对象
     */
    public Double getDouble(String path, double defaultValue)
    {
        Double result = this.getDouble(path);
        if (null == result) {
            return defaultValue;
        }
        return result;
    }

    /**
     * 根据用户提供的json path，寻址List对象，如果对象不存在，返回null
     *
     * @param path String json 路径
     *
     * @return 对象列表
     */
    @SuppressWarnings("unchecked")
    public List<Object> getList(String path)
    {
        return this.get(path, List.class);
    }

    /**
     * 根据用户提供的json path，寻址List对象，如果对象不存在，返回null
     *
     * @param path json 路径
     * @param t 要转换的类型
     * @param <T> 类型
     * @return 列表
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(String path, Class<T> t)
    {
        List<Object> object = this.get(path, List.class);
        if (null == object) {
            return Collections.emptyList();
        }

        List<T> result = new ArrayList<>();

        List<Object> origin;
        try {
            origin = object;
        }
        catch (ClassCastException e) {
            // .warn("{} 转为 List 时发生了异常，默认将此值添加到 List 中", String.valueOf(object))
            origin = new ArrayList<>();
            origin.add(String.valueOf(object));
        }

        for (Object each : origin) {
            result.add((T) each);
        }

        return result;
    }

    /**
     * 根据用户提供的json path，寻址List对象，如果对象不存在，返回默认List
     *
     * @param path json 路径
     * @param defaultList 默认返回的值
     *
     * @return 对象列表
     */
    public List<Object> getList(String path, List<Object> defaultList)
    {
        List<Object> object = this.getList(path);
        if (null == object) {
            return defaultList;
        }
        return object;
    }

    /**
     * 根据用户提供的json path，寻址List对象，如果对象不存在，返回默认List
     *
     * @param path String json 路径
     * @param defaultList 默认返回的值
     * @param t 要转换的类型
     * @param <T> 类型
     *
     * @return 列表
     */
    public <T> List<T> getList(String path, List<T> defaultList,
            Class<T> t)
    {
        List<T> list = this.getList(path, t);
        if (null == list) {
            return defaultList;
        }
        return list;
    }

    /**
     * 根据用户提供的json path，寻址包含Configuration的List，如果对象不存在，返回默认null
     *
     * @param path String json 路径
     *
     * @return 列表
     */
    public List<Configuration> getListConfiguration(String path)
    {
        List<Object> lists = getList(path);
        if (lists == null) {
            return Collections.emptyList();
        }

        List<Configuration> result = new ArrayList<>();
        for (Object object : lists) {
            result.add(Configuration.from(Configuration.toJSONString(object)));
        }
        return result;
    }

    /**
     * 根据用户提供的json path，寻址Map对象，如果对象不存在，返回null
     *
     * @param path String json 路径
     *
     * @return map 对象
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String path)
    {
        return this.get(path, Map.class);
    }

    /**
     * 根据用户提供的json path，寻址Map对象，如果对象不存在，返回默认map
     *
     * @param path String json 路径
     * @param defaultMap 默认返回值
     *
     * @return map对象
     */
    public Map<String, Object> getMap(String path,
            Map<String, Object> defaultMap)
    {
        Map<String, Object> object = this.getMap(path);
        if (null == object) {
            return defaultMap;
        }
        return object;
    }

    /**
     * 根据用户提供的json path，寻址具体的对象，并转为用户提供的类型
     * NOTE: 目前仅支持Map以及List下标寻址, 例如:
     * 对于如下JSON
     * <p>
     * {"a": {"b": {"c": [0,1,2,3]}}}
     * </p>
     * config.get("") 返回整个Map <br>
     * config.get("a") 返回a下属整个Map <br>
     * config.get("a.b.c") 返回c对应的数组List <br>
     * config.get("a.b.c[0]") 返回数字0
     *
     * @param path String json 路径
     * @param clazz Class 要转换的类型
     * @param <T> 类型
     *
     * @return Java表示的JSON对象，如果转型失败，将抛出异常
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String path, Class<T> clazz)
    {
        this.checkPath(path);
        return (T) this.get(path);
    }

    /**
     * 格式化Configuration输出
     *
     * @return string
     */
    public String beautify()
    {
        return JSON.toJSONString(this.getInternal(), JSONWriter.Feature.PrettyFormat);
    }

    /**
     * 根据用户提供的json path，插入指定对象，并返回之前存在的对象(如果存在)
     *
     * 目前仅支持.以及数组下标寻址, 例如:
     * <p>
     * config.set("a.b.c[3]", object);
     * </p>
     * 对于插入对象，Configuration 不做任何限制，但是请务必保证该对象是简单对象，不要使用自定义对象，否则后续对于JSON序列化等情况会出现未定义行为。
     *
     * @param path JSON path对象
     * @param object 需要插入的对象
     *
     * @return Java表示的JSON对象
     */
    public Object set(String path, Object object)
    {
        checkPath(path);

        Object result = this.get(path);

        setObject(path, extractConfiguration(object));

        return result;
    }

    /**
     * 获取Configuration下所有叶子节点的key
     * 对于
     * <p>
     * {"a": {"b": {"c": [0,1,2,3]}}, "x": "y"}
     * </p>
     * 下属的key包括: a.b.c[0],a.b.c[1],a.b.c[2],a.b.c[3],x
     *
     * @return set of string
     */
    public Set<String> getKeys()
    {
        Set<String> collect = new HashSet<>();
        this.getKeysRecursive(this.getInternal(), "", collect);
        return collect;
    }

    /**
     * 删除path对应的值，如果path不存在，将抛出异常。
     *
     * @param path String json 路径
     *
     * @return Object 返回查到的对象，若不存在则返回为空
     */
    public Object remove(String path)
    {
        Object result = this.get(path);
        if (null == result) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.RUNTIME_ERROR,
                    String.format("配置文件对应Key[%s]并不存在，该情况是代码编程错误.", path));
        }

        this.set(path, null);
        return result;
    }

    /**
     * 合并其他Configuration，并修改两者冲突的KV配置
     *
     * @param another Configuration 合并加入的第三方Configuration
     * @param updateWhenConflict boolean 当合并双方出现KV冲突时候，选择更新当前KV，或者忽略该KV
     *
     * @return Configuration
     */
    public Configuration merge(Configuration another,
            boolean updateWhenConflict)
    {
        Set<String> keys = another.getKeys();

        // 如果使用更新策略，凡是another存在的key，均需要更新
        // 使用忽略策略，只有another Configuration存在但是当前Configuration不存在的key，才需要更新
        keys.forEach(key -> {
            if (updateWhenConflict) {
                this.set(key, another.get(key));
                return;
            }
            boolean isCurrentExists = this.get(key) != null;
            if (isCurrentExists) {
                return;
            }
            this.set(key, another.get(key));
        });
        return this;
    }

    @Override
    public String toString()
    {
        return this.toJSON();
    }

    /**
     * 将Configuration作为JSON输出
     *
     * @return String json string
     */
    public String toJSON()
    {
        return Configuration.toJSONString(this.getInternal());
    }

    /**
     * 拷贝当前Configuration，注意，这里使用了深拷贝，避免冲突
     */
    @Override
    public Configuration clone()
    {
        Configuration config = Configuration
                .from(Configuration.toJSONString(this.getInternal()));
        config.addSecretKeyPath(this.secretKeyPathSet);
        return config;
    }

    /**
     * 按照configuration要求格式的path
     * 比如：
     * a.b.c
     * a.b[2].c
     *
     * @param path 路径
     *
     */
    public void addSecretKeyPath(String path)
    {
        if (StringUtils.isNotBlank(path)) {
            this.secretKeyPathSet.add(path);
        }
    }

    public void addSecretKeyPath(Set<String> pathSet)
    {
        if (pathSet != null) {
            this.secretKeyPathSet.addAll(pathSet);
        }
    }

    @SuppressWarnings("unchecked")
    void getKeysRecursive(Object current, String path, Set<String> collect)
    {
        boolean isRegularElement = !(current instanceof Map || current instanceof List);
        if (isRegularElement) {
            collect.add(path);
            return;
        }

        boolean isMap = current instanceof Map;
        if (isMap) {
            Map<String, Object> mapping = ((Map<String, Object>) current);
            mapping.keySet().forEach(key -> {
                if (StringUtils.isBlank(path)) {
                    getKeysRecursive(mapping.get(key), key.trim(), collect);
                }
                else {
                    getKeysRecursive(mapping.get(key), path + "." + key.trim(),
                            collect);
                }
            });
            return;
        }

        List<Object> lists = (List<Object>) current;
        for (int i = 0; i < lists.size(); i++) {
            getKeysRecursive(lists.get(i), path + String.format("[%d]", i),
                    collect);
        }
    }

    public Object getInternal()
    {
        return this.root;
    }

    private void setObject(String path, Object object)
    {
        Object newRoot = setObjectRecursive(this.root, split2List(path), 0,
                object);

        if (isSuitForRoot(newRoot)) {
            this.root = newRoot;
            return;
        }

        throw AddaxException.asAddaxException(CommonErrorCode.RUNTIME_ERROR,
                String.format("值[%s]无法适配您提供[%s]， 该异常代表系统编程错误.",
                        ToStringBuilder.reflectionToString(object), path));
    }

    @SuppressWarnings("unchecked")
    private Object extractConfiguration(Object object)
    {
        if (object instanceof Configuration) {
            return extractFromConfiguration(object);
        }

        if (object instanceof List) {
            List<Object> result = new ArrayList<>();
            for (Object each : (List<Object>) object) {
                result.add(extractFromConfiguration(each));
            }
            return result;
        }

        if (object instanceof Map) {
            Map<String, Object> result = new HashMap<>();
            for (String key : ((Map<String, Object>) object).keySet()) {
                result.put(key,
                        extractFromConfiguration(((Map<String, Object>) object)
                                .get(key)));
            }
            return result;
        }

        return object;
    }

    private Object extractFromConfiguration(Object object)
    {
        if (object instanceof Configuration) {
            return ((Configuration) object).getInternal();
        }

        return object;
    }

    Object buildObject(List<String> paths, Object object)
    {
        if (null == paths) {
            throw AddaxException.asAddaxException(
                    CommonErrorCode.RUNTIME_ERROR,
                    "Path不能为null，该异常代表系统编程错误.");
        }

        if (1 == paths.size() && StringUtils.isBlank(paths.get(0))) {
            return object;
        }

        Object child = object;
        for (int i = paths.size() - 1; i >= 0; i--) {
            String path = paths.get(i);

            if (isPathMap(path)) {
                Map<String, Object> mapping = new HashMap<>();
                mapping.put(path, child);
                child = mapping;
                continue;
            }

            if (isPathList(path)) {
                List<Object> lists = new ArrayList<>(
                        this.getIndex(path) + 1);
                expand(lists, this.getIndex(path) + 1);
                lists.set(this.getIndex(path), child);
                child = lists;
                continue;
            }

            throw AddaxException.asAddaxException(
                    CommonErrorCode.RUNTIME_ERROR, String.format(
                            "路径[%s]出现非法值类型[%s]，该异常代表系统编程错误. .",
                            StringUtils.join(paths, "."), path));
        }

        return child;
    }

    @SuppressWarnings("unchecked")
    Object setObjectRecursive(Object current, List<String> paths,
            int index, Object value)
    {

        // 如果是已经超出path，我们就返回value即可，作为最底层叶子节点
        boolean isLastIndex = index == paths.size();
        if (isLastIndex) {
            return value;
        }

        String path = paths.get(index).trim();
        boolean isNeedMap = isPathMap(path);
        if (isNeedMap) {
            Map<String, Object> mapping;

            // 当前不是map，因此全部替换为map，并返回新建的map对象
            boolean isCurrentMap = current instanceof Map;
            if (!isCurrentMap) {
                mapping = new HashMap<>();
                mapping.put(
                        path,
                        buildObject(paths.subList(index + 1, paths.size()),
                                value));
                return mapping;
            }

            // 当前是map，但是没有对应的key，也就是我们需要新建对象插入该map，并返回该map
            mapping = ((Map<String, Object>) current);
            boolean hasSameKey = mapping.containsKey(path);
            if (!hasSameKey) {
                mapping.put(
                        path,
                        buildObject(paths.subList(index + 1, paths.size()),
                                value));
                return mapping;
            }

            // 当前是map，而且还竟然存在这个值，好吧，继续递归遍历
            current = mapping.get(path);
            mapping.put(path,
                    setObjectRecursive(current, paths, index + 1, value));
            return mapping;
        }

        boolean isNeedList = isPathList(path);
        if (isNeedList) {
            List<Object> lists;
            int listIndexer = getIndex(path);

            // 当前是list，直接新建并返回即可
            boolean isCurrentList = current instanceof List;
            if (!isCurrentList) {
                lists = expand(new ArrayList<>(), listIndexer + 1);
                lists.set(
                        listIndexer,
                        buildObject(paths.subList(index + 1, paths.size()),
                                value));
                return lists;
            }

            // 当前是list，但是对应的indexer是没有具体的值，也就是我们新建对象然后插入到该list，并返回该List
            lists = expand((List<Object>) current, listIndexer + 1);

            boolean hasSameIndex = lists.get(listIndexer) != null;
            if (!hasSameIndex) {
                lists.set(
                        listIndexer,
                        buildObject(paths.subList(index + 1, paths.size()),
                                value));
                return lists;
            }

            // 当前是list，并且存在对应的index，没有办法继续递归寻找
            current = lists.get(listIndexer);
            lists.set(listIndexer,
                    setObjectRecursive(current, paths, index + 1, value));
            return lists;
        }

        throw AddaxException.asAddaxException(CommonErrorCode.RUNTIME_ERROR,
                "该异常代表系统编程错误.");
    }

    private Object findObject(String path)
    {
        boolean isRootQuery = StringUtils.isBlank(path);
        if (isRootQuery) {
            return this.root;
        }

        Object target = this.root;

        for (String each : split2List(path)) {
            if (isPathMap(each)) {
                target = findObjectInMap(target, each);
            }
            else {
                target = findObjectInList(target, each);
            }
        }

        return target;
    }

    @SuppressWarnings("unchecked")
    private Object findObjectInMap(Object target, String index)
    {
        boolean isMap = (target instanceof Map);
        if (!isMap) {
            throw new IllegalArgumentException(String.format(
                    "您提供的配置文件有误. 路径[%s]需要配置Json格式的Map对象，但该节点发现实际类型是[%s]. 请检查您的配置并作出修改.",
                    index, target.getClass().toString()));
        }

        Object result = ((Map<String, Object>) target).get(index);
        if (null == result) {
            throw new IllegalArgumentException(String.format(
                    "您提供的配置文件有误. 路径[%s]值为null，无法识别该配置. 请检查您的配置并作出修改.", index));
        }

        return result;
    }

    @SuppressWarnings({"unchecked"})
    private Object findObjectInList(Object target, String each)
    {
        boolean isList = (target instanceof List);
        if (!isList) {
            throw new IllegalArgumentException(String.format(
                    "您提供的配置文件有误. 路径[%s]需要配置Json格式的Map对象，但该节点发现实际类型是[%s]. 请检查您的配置并作出修改.",
                    each, target.getClass().toString()));
        }

        String index = each.replace("[", "").replace("]", "");
        if (!StringUtils.isNumeric(index)) {
            throw new IllegalArgumentException(
                    String.format(
                            "系统编程错误，列表下标必须为数字类型，但该节点发现实际类型是[%s] ，该异常代表系统编程错误.",
                            index));
        }

        return ((List<Object>) target).get(Integer.parseInt(index));
    }

    private List<Object> expand(List<Object> list, int size)
    {
        int expand = size - list.size();
        while (expand-- > 0) {
            list.add(null);
        }
        return list;
    }

    private boolean isPathList(String path)
    {
        return path.contains("[") && path.contains("]");
    }

    private boolean isPathMap(String path)
    {
        return StringUtils.isNotBlank(path) && !isPathList(path);
    }

    private int getIndex(String index)
    {
        return Integer.parseInt(index.replace("[", "").replace("]", ""));
    }

    private boolean isSuitForRoot(Object object)
    {
        return (object instanceof List || object instanceof Map);
    }

    private String split(String path)
    {
        return StringUtils.replace(path, "[", ".[");
    }

    private List<String> split2List(String path)
    {
        return Arrays.asList(StringUtils.split(split(path), "."));
    }

    private void checkPath(String path)
    {
        if (null == path) {
            throw new IllegalArgumentException(
                    "系统编程错误, 该异常代表系统编程错误..");
        }

        for (String each : StringUtils.split(".")) {
            if (StringUtils.isBlank(each)) {
                throw new IllegalArgumentException(String.format(
                        "系统编程错误, 路径[%s]不合法, 路径层次之间不能出现空白字符 .", path));
            }
        }
    }

    public Set<String> getSecretKeyPathSet()
    {
        return secretKeyPathSet;
    }

    public void setSecretKeyPathSet(Set<String> keyPathSet)
    {
        if (keyPathSet != null) {
            this.secretKeyPathSet = keyPathSet;
        }
    }
}
