package me.smecsia.cassajem.util;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.SystemPropertyUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Util class allowing to scan all the classes inside the specified package and other class operations
 * User: smecsia
 * Date: 16.03.12
 * Time: 15:55
 */
public class ClassUtil {

    public static String resolveBasePackage(String basePackage) {
        return ClassUtils.convertClassNameToResourcePath(SystemPropertyUtils.resolvePlaceholders(basePackage));
    }

    private static final ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @param basePackage base package name
     * @return list of the classes within base package
     * @throws java.io.IOException
     * @throws ClassNotFoundException
     */
    public static List<Class> getClasses(String basePackage) throws IOException, ClassNotFoundException {
        MetadataReaderFactory metadataReaderFactory = new CachingMetadataReaderFactory(resourcePatternResolver);

        List<Class> candidates = new ArrayList<Class>();
        String packageSearchPath = ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX +
                resolveBasePackage(basePackage) + "/" + "**/*.class";
        Resource[] resources = resourcePatternResolver.getResources(packageSearchPath);
        for (Resource resource : resources) {
            if (resource.isReadable()) {
                MetadataReader metadataReader = metadataReaderFactory.getMetadataReader(resource);
                candidates.add(Class.forName(metadataReader.getClassMetadata().getClassName()));
            }
        }
        return candidates;
    }


    /**
     * Recursive method used to find all classes in a given directory and subdirs.
     *
     * @param directory   The base directory
     * @param packageName The package name for classes found inside the base directory
     * @return The classes
     * @throws ClassNotFoundException
     */
    public static List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
        List<Class> classes = new ArrayList<Class>();
        if (!directory.exists()) {
            return classes;
        }
        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file, packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
            }
        }
        return classes;
    }

    /**
     * Returns list of resources defined in classpath by a pattern
     * @param pattern pattern
     * @return list of Strings URLs of resources
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static Collection<String> resolveResourcesAsStringsFromPattern(final String pattern)
            throws IOException, ClassNotFoundException {
        final Collection<String> classes = new LinkedList<String>();
        final Resource[] resources = resourcePatternResolver.getResources(pattern);
        for (final Resource resource : resources) {
            final URL url = resource.getURL();
            classes.add(url.toString());
        }
        return classes;
    }


    /**
     * Returns list of resources defined in classpath by a pattern
     * @param pattern pattern
     * @return list of resources
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static Collection<Resource> resolveResourcesFromPattern(final String pattern)
            throws IOException, ClassNotFoundException {
        final Collection<Resource> classes = new LinkedList<Resource>();
        final Resource[] resources = resourcePatternResolver.getResources(pattern);
        for (final Resource resource : resources) {
            classes.add(resource);
        }
        return classes;
    }
}
