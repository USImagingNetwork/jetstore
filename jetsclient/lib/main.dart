import 'package:flutter/material.dart';
import 'package:adaptive_theme/adaptive_theme.dart';
import 'package:provider/provider.dart';
import 'package:jetsclient/http_client.dart';

import 'package:jetsclient/routes/jets_route_information_parser.dart';
import 'package:jetsclient/routes/jets_router_delegate.dart';

final jetsRouteDelegate = JetsRouterDelegate();
final jetsRouteInformationParser = JetsRouteInformationParser();

void main() {
  runApp(const JetsClient(serverOrigin: 'http://localhost:8080'));
}

class JetsClient extends StatefulWidget {
  final String serverOrigin;
  const JetsClient({required this.serverOrigin, super.key});

  @override
  State<JetsClient> createState() => JetsClientState();
}

class JetsClientState extends State<JetsClient> {
  // This widget is the root of your application.
  @override
  Widget build(BuildContext context) {
    return AdaptiveTheme(
      light: ThemeData(
          brightness: Brightness.light,
          colorSchemeSeed: const Color.fromRGBO(118, 219, 21, 1.0)),
      dark: ThemeData(
          brightness: Brightness.dark,
          colorSchemeSeed: const Color.fromRGBO(118, 219, 21, 1.0)),
      initial: AdaptiveThemeMode.light,
      builder: (theme, darkTheme) => MultiProvider(
        providers: [
          // // In this sample app, CatalogModel never changes, so a simple Provider
          // // is sufficient.
          // Provider(create: (context) => CatalogModel()),
          // // CartModel is implemented as a ChangeNotifier, which calls for the use
          // // of ChangeNotifierProvider. Moreover, CartModel depends
          // // on CatalogModel, so a ProxyProvider is needed.
          // ChangeNotifierProxyProvider<CatalogModel, CartModel>(
          //   create: (context) => CartModel(),
          //   update: (context, catalog, cart) {
          //     if (cart == null) throw ArgumentError.notNull('cart');
          //     cart.catalog = catalog;
          //     return cart;
          //   },
          // ),
          // http Client
          Provider(create: (context) => HttpClient(widget.serverOrigin)),
        ],
        child: MaterialApp.router(
          title: 'JetStore Client',
          theme: theme,
          routerDelegate: jetsRouteDelegate,
          routeInformationParser: jetsRouteInformationParser,
          // initialRoute: '/login',
          // routes: {
          //   '/': (context) => const WelcomeScreen(),
          //   '/login': (context) => const LoginScreen(),
          //   '/register': (context) => const RegistrationScreen(),
          //   // '/catalog': (context) => const MyCatalog(),
          //   // '/cart': (context) => const MyCart(),
          // },
        ),
      ),
    );
  }
}

// class WelcomeScreen extends StatelessWidget {
//   const WelcomeScreen({super.key});

//   @override
//   Widget build(BuildContext context) {
//     return Scaffold(
//       appBar: AppBar(
//         automaticallyImplyLeading: false,
//         title: const Text('JetStore Workspace'),
//         actions: <Widget>[
//           IconButton(
//             icon: const Icon(Icons.dark_mode_sharp),
//             tooltip: 'Toggle Theme',
//             onPressed: () {
//               AdaptiveTheme.of(context).toggleThemeMode();
//             },
//           ),
//           IconButton(
//             icon: const Icon(Icons.logout_sharp),
//             tooltip: 'Log Out',
//             onPressed: () {
//               var user = context.read<UserModel>();
//               user.name = "";
//               user.email = "";
//               user.password = "";
//               user.token = "";
//               Navigator.pushReplacementNamed(context, '/login');
//             },
//           ),
//         ],
//       ),
//       body: Center(
//         child: Text('Welcome!', style: Theme.of(context).textTheme.headline2),
//       ),
//     );
//   }
// }