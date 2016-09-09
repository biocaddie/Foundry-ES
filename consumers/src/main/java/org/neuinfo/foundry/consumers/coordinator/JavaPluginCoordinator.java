package org.neuinfo.foundry.consumers.coordinator;

import org.json.JSONObject;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 10/27/14.
 */
public class JavaPluginCoordinator {
    URLClassLoader pluginClassLoader;
    String pluginDir;
    String libDir;
    List<File> libJars;
    List<File> pluginJars;
    private static JavaPluginCoordinator instance = null;

    public synchronized static JavaPluginCoordinator getInstance(String pluginDir, String libDir) throws MalformedURLException {
        if (instance == null) {
            instance = new JavaPluginCoordinator(pluginDir, libDir);
            instance.initialize();
        }
        return instance;
    }

    public synchronized static JavaPluginCoordinator getInstance() {
        if (instance == null) {
            throw new RuntimeException("JavaPluginCoordinator not initialized properly!");
        }
        return instance;
    }

    private JavaPluginCoordinator(String pluginDir, String libDir) {
        this.pluginDir = pluginDir;
        this.libDir = libDir;
        File[] files = new File(libDir).listFiles();
        int len = files != null ? files.length : 0;
        this.libJars = new ArrayList<File>(len);
        if (files != null) {
            for (File f : files) {
                if (f.getName().endsWith(".jar")) {
                    libJars.add(f);
                }
            }
        }
        files = new File(pluginDir).listFiles();
        len = files != null ? files.length : 0;
        this.pluginJars = new ArrayList<File>(len);
        if (files != null) {
            for (File f : files) {
                if (f.getName().endsWith(".jar")) {
                    pluginJars.add(f);
                }
            }
        }

    }

    private void initialize() throws MalformedURLException {
        URL[] urls = new URL[libJars.size() + pluginJars.size()];
        int i = 0;
        for (File libJar : libJars) {
            urls[i++] = libJar.toURI().toURL();
        }
        for (File pluginJar : pluginJars) {
            urls[i++] = pluginJar.toURI().toURL();
        }

        pluginClassLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
    }


    public Object runMethod(String fullClassName, String methodName, Class<?>[] parameters, Object[] args) throws Exception {
        Class<?> clazz = Class.forName(fullClassName, true, this.pluginClassLoader);
        Method method = clazz.getDeclaredMethod(methodName, parameters);
        Object instance = clazz.newInstance();
        if (args == null) {
            return method.invoke(instance);
        }
        return method.invoke(instance, args);
    }

    public Object createInstance(String fullClassName) throws Exception {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(fullClassName, true, this.pluginClassLoader);
        } catch (ClassNotFoundException cnfe) {
            clazz = Class.forName(fullClassName);
        }
        Object instance = clazz.newInstance();
        return instance;
    }

    public Object runMethod(Object instance, String methodName, Class<?>[] parameters, Object[] args) throws Exception {
        Method method;
        if (parameters != null) {
            method = instance.getClass().getDeclaredMethod(methodName, parameters);
        } else {
            method = instance.getClass().getDeclaredMethod(methodName);
        }
        if (args == null) {
            return method.invoke(instance);
        }
        return method.invoke(instance, args);
    }


    public static class JarInfo {
        String jarPath;
        String crc;
        String jarName;

        public JarInfo(String jarPath, String crc, String jarName) {
            this.jarPath = jarPath;
            this.crc = crc;
            this.jarName = jarName;
        }

        public String getJarPath() {
            return jarPath;
        }

        public String getCrc() {
            return crc;
        }

        public String getJarName() {
            return jarName;
        }
    }

    public static void main(String[] args) throws Exception {
        String libDir = "/tmp/plugin_test/lib";
        String pluginDir = "/tmp/plugin_test/plugins";

        JavaPluginCoordinator c = JavaPluginCoordinator.getInstance(pluginDir, libDir);

        Object instance = c.createInstance("org.neuinfo.consumers.ElasticSearchIndexDocWithResourcePreparer");

        JSONObject origContent = new JSONObject();
        origContent.put("description", "some description");
        JSONObject data = new JSONObject();
        Class<?>[] params = new Class<?>[]{JSONObject.class, JSONObject.class};
        JSONObject js = (JSONObject) c.runMethod(instance, "handle", params, new Object[]{origContent, data});

        System.out.println(js.toString(2));

    }
}
