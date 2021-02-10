package com.wgzhao.datax.core.util;

import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.core.util.container.CoreConstant;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class ConfigParser
{
    private static final Logger LOG = LoggerFactory.getLogger(ConfigParser.class);

    private ConfigParser() {}

    /*
     * 指定Job配置路径，ConfigParser会解析Job、Plugin、Core全部信息，并以Configuration返回
     */
    public static Configuration parse(String jobPath)
    {
        Configuration configuration = ConfigParser.parseJobConfig(jobPath);

        configuration.merge(
                ConfigParser.parseCoreConfig(CoreConstant.DATAX_CONF_PATH),
                false);
        String readerPluginName = configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        String writerPluginName = configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);

        String preHandlerName = configuration.getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);

        String postHandlerName = configuration.getString(
                CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);

        Set<String> pluginList = new HashSet<>();
        pluginList.add(readerPluginName);
        pluginList.add(writerPluginName);

        if (StringUtils.isNotEmpty(preHandlerName)) {
            pluginList.add(preHandlerName);
        }
        if (StringUtils.isNotEmpty(postHandlerName)) {
            pluginList.add(postHandlerName);
        }
        try {
            configuration.merge(parsePluginConfig(new ArrayList<>(pluginList)),
                    false);
        }
        catch (Exception e) {
            //吞掉异常，保持log干净。这里message足够。
            LOG.warn(String.format("插件[%s,%s]加载失败，1s后重试... Exception:%s ",
                    readerPluginName, writerPluginName, e.getMessage()));
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e1) {
                //
            }
            configuration.merge(parsePluginConfig(new ArrayList<>(pluginList)),
                    false);
        }

        return configuration;
    }

    private static Configuration parseCoreConfig(String path)
    {
        return Configuration.from(new File(path));
    }

    public static Configuration parseJobConfig(String path)
    {
        String jobContent = getJobContent(path);
        return Configuration.from(jobContent);
    }

    private static String getJobContent(String jobResource)
    {
        String jobContent;

        boolean isJobResourceFromHttp = jobResource.trim().toLowerCase().startsWith("http");

        if (isJobResourceFromHttp) {
            //设置httpclient的 HTTP_TIMEOUT_INMILLIONSECONDS
            Configuration coreConfig = ConfigParser.parseCoreConfig(CoreConstant.DATAX_CONF_PATH);
            int httpTimeOutInMillionSeconds = coreConfig.getInt(
                    CoreConstant.DATAX_CORE_DATAXSERVER_TIMEOUT, 5000);
            HttpClientUtil.setHttpTimeoutInMillionSeconds(httpTimeOutInMillionSeconds);

            HttpClientUtil httpClientUtil = new HttpClientUtil();
            try {
                URL url = new URL(jobResource);
                HttpGet httpGet = HttpClientUtil.getGetRequest();
                httpGet.setURI(url.toURI());

                jobContent = httpClientUtil.executeAndGetWithFailedRetry(
                        httpGet, 1, 1000L);
            }
            catch (Exception e) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                        "获取作业配置信息失败:" + jobResource, e);
            }
        }
        else {
            // jobResource 是本地文件绝对路径
            try {
                jobContent = FileUtils.readFileToString(new File(jobResource), StandardCharsets.UTF_8);
            }
            catch (IOException e) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                        "获取作业配置信息失败:" + jobResource, e);
            }
        }

        if (jobContent == null) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    "获取作业配置信息失败:" + jobResource);
        }
        return jobContent;
    }

    public static Configuration parsePluginConfig(List<String> wantPluginNames)
    {
        Configuration configuration = Configuration.newDefault();

        Set<String> replicaCheckPluginSet = new HashSet<>();
        int complete = 0;
        for (String each : ConfigParser
                .getDirAsList(CoreConstant.DATAX_PLUGIN_READER_HOME)) {
            Configuration eachReaderConfig = ConfigParser.parseOnePluginConfig(each,
                    "reader", replicaCheckPluginSet, wantPluginNames);
            if (eachReaderConfig != null) {
                configuration.merge(eachReaderConfig, true);
                complete += 1;
            }
        }

        for (String each : ConfigParser
                .getDirAsList(CoreConstant.DATAX_PLUGIN_WRITER_HOME)) {
            Configuration eachWriterConfig = ConfigParser.parseOnePluginConfig(each,
                    "writer", replicaCheckPluginSet, wantPluginNames);
            if (eachWriterConfig != null) {
                configuration.merge(eachWriterConfig, true);
                complete += 1;
            }
        }

        if (wantPluginNames != null && !wantPluginNames.isEmpty()
                && wantPluginNames.size() != complete) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_INIT_ERROR,
                    "插件加载失败，未完成指定插件加载:" + wantPluginNames);
        }

        return configuration;
    }

    public static Configuration parseOnePluginConfig(String path, String type,
            Set<String> pluginSet, List<String> wantPluginNames)
    {
        String filePath = path + File.separator + "plugin.json";
        Configuration configuration = Configuration.from(new File(filePath));

        String pluginPath = configuration.getString("path");
        String pluginName = configuration.getString("name");
        if (!pluginSet.contains(pluginName)) {
            pluginSet.add(pluginName);
        }
        else {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_INIT_ERROR,
                    "插件加载失败,存在重复插件:" + filePath);
        }

        //不是想要的插件，返回null
        if (wantPluginNames != null && !wantPluginNames.isEmpty() && !wantPluginNames.contains(pluginName)) {
            return null;
        }

        boolean isDefaultPath = StringUtils.isBlank(pluginPath);
        if (isDefaultPath) {
            configuration.set("path", path);
        }

        Configuration result = Configuration.newDefault();

        result.set(
                String.format("plugin.%s.%s", type, pluginName),
                configuration.getInternal());

        return result;
    }

    private static List<String> getDirAsList(String path)
    {
        List<String> result = new ArrayList<>();

        String[] paths = new File(path).list();
        if (null == paths) {
            return result;
        }

        for (String each : paths) {
            result.add(path + File.separator + each);
        }

        return result;
    }
}