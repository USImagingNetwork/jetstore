import 'package:flutter/material.dart';
import 'package:jetsclient/screens/components/app_bar.dart';
import 'package:jetsclient/screens/components/data_table.dart';
import 'package:jetsclient/utils/constants.dart';

final List<String> menuEntries = <String>[
  'Input Files',
  'Mapping Configurations',
  'Process Configurations',
  'Data Pipelines'
];
final List<VoidCallback> menuActions = <VoidCallback>[
  () {},
  () {},
  () {},
  () {}
];

class JobListScreen extends StatefulWidget {
  const JobListScreen({super.key});

  @override
  State<JobListScreen> createState() => _JobListScreenState();
}

class _JobListScreenState extends State<JobListScreen> {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: appBar('JetStore Workspace', context),
      body: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Flexible(
            flex: 1,
            fit: FlexFit.tight,
            child: Column(children: [
              const SizedBox(height: defaultPadding),
              Expanded(
                flex: 1,
                child: Image.asset('assets/images/logo.png'),
              ),
              const SizedBox(height: defaultPadding),
              Expanded(
                  flex: 8,
                  child: ListView.separated(
                    padding: const EdgeInsets.all(defaultPadding),
                    itemCount: menuEntries.length,
                    itemBuilder: (BuildContext context, int index) {
                      return ElevatedButton(
                        style: ElevatedButton.styleFrom(
                          // Foreground color
                          onPrimary: Theme.of(context)
                              .colorScheme
                              .onSecondaryContainer,
                          // Background color
                          primary:
                              Theme.of(context).colorScheme.secondaryContainer,
                        ).copyWith(elevation: ButtonStyleButton.allOrNull(0.0)),
                        onPressed: menuActions[index],
                        child: Center(child: Text(menuEntries[index])),
                      );
                    },
                    separatorBuilder: (BuildContext context, int index) =>
                        const Divider(),
                  ))
            ]),
          ),
          Flexible(
            flex: 5,
            fit: FlexFit.tight,
            child:
                Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
              const SizedBox(height: 2 * defaultPadding),
              Flexible(
                flex: 1,
                fit: FlexFit.tight,
                child: Text(
                  'Data Pipelines',
                  style: Theme.of(context).textTheme.headline4,
                ),
              ),
              const Flexible (
                flex: 8,
                fit: FlexFit.tight,
                child: JetsDataTableWidget(tableConfig: "joblist"),
              ),
            ]),
          ),
        ],
      ),
    );
  }
}