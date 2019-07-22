# Introduction
Expat is a tool of the Hopsworks ecosystem which is used for upgrading Hopsworks existing installations.
Expat is versioned and it follows the same versions as Hopsworks. The migration steps to be executed are defined in 
``resources/expat-site-default.xml`` under the ``<version-*>`` tags. When upgrading an existing Hopsworks cluster, 
the cluster definition contains the current version and the version to upgrade to. Expat will run all migration steps
 that fall between the two Hopsworks versions, excluding the current version of course. For example. If the current 
 Hopsworks cluster to be upgraded is running version ``0.6.1`` and you are upgrading to ``1.0.0``, steps ``070``, 
 ``010`` and ``10`` will be executed. Bug fix version upgrades are currently not supported, you can track progress of
  by following [HOPSWORKS-814](https://logicalclocks.atlassian.net/browse/HOPSWORKS-814).


#Adding your migration step
You need to define a class that implements the ``MigrateStep`` interface. Be careful to add the ``ShutdownHook`` in 
your class and in the main method in ``Expat``, if needed.

#Build
``mvn clean package``

#Run
Follow the steps of the [migrate](https://github.com/logicalclocks/hopsworks-chef/blob/master/recipes/migrate.rb) recipe.
