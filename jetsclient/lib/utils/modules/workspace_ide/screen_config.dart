import 'package:jetsclient/routes/jets_routes_app.dart';
import 'package:jetsclient/utils/constants.dart';
import 'package:jetsclient/utils/screen_config.dart';

//*TODO Take path params from current Navigator provider (current page)
final List<MenuEntry> workspaceRegistryMenuEntries = [
  MenuEntry(
      key: 'jetstoreHome',
      label: 'JetStore Home',
      routePath: homePath),
  MenuEntry(
      key: 'workspaceIDEHome',
      label: 'Workspace IDE Home',
      routePath: workspaceRegistryPath),
];
final List<MenuEntry> workspaceIDEMenuEntries = [
  MenuEntry(
      key: 'workspaceIDEHome',
      label: 'Select Another Workspace',
      routePath: workspaceRegistryPath),
  MenuEntry(
      key: 'domainClasses',
      label: 'Domain Classes',
      routePath: wsDomainClassesPath),
  MenuEntry(
      key: 'domainTables',
      label: 'Domain Tables',
      routePath: wsDomainTablesPath),
];

final Map<String, ScreenConfig> _screenConfigurations = {
  // workspaceRegistry Screen
  ScreenKeys.workspaceRegistry: ScreenConfig(
      key: ScreenKeys.workspaceRegistry,
      appBarLabel: 'JetStore Workspace IDE',
      title: 'Welcome to JetStore Workspace IDE!',
      showLogout: true,
      leftBarLogo: 'assets/images/logo.png',
      menuEntries: workspaceRegistryMenuEntries),

  // domainClasses Screen
  ScreenKeys.wsDomainClasses: ScreenConfig(
      key: ScreenKeys.wsDomainClasses,
      appBarLabel: 'JetStore Workspace IDE',
      title: 'Domain Classes',
      showLogout: true,
      leftBarLogo: 'assets/images/logo.png',
      menuEntries: workspaceIDEMenuEntries),
};


ScreenConfig? getWorkspaceScreenConfig(String key) {
  var config = _screenConfigurations[key];
  return config;
}