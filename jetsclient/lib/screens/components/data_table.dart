import 'package:flutter/material.dart';
import 'package:jetsclient/utils/data_table_config.dart';
import 'package:jetsclient/utils/constants.dart';

//* examples
typedef FncBool = void Function(bool?);
typedef OnSelectCB = void Function(bool value, int index);

class JetsDataTableWidget extends StatefulWidget {
  const JetsDataTableWidget({super.key, required this.tableConfig});
  final String tableConfig;

  @override
  State<JetsDataTableWidget> createState() => _JetsDataTableState();
}

class _JetsDataTableState extends State<JetsDataTableWidget> {
  @override
  void initState() {
    super.initState();
    tableConfig = getTableConfig(widget.tableConfig);
    sortColumnIndex = tableConfig!.sortColumnIndex;
    sortAscending = tableConfig!.sortAscending;
    rowsPerPage = tableConfig!.rowsPerPage;
    selectedRows = List<bool>.filled(tableConfig!.rowsPerPage, false);
  }

  final ScrollController _verticalController = ScrollController();
  final ScrollController _horizontalController = ScrollController();
  static const int numItems = 10;
  var selectedRows = <bool>[];
  bool isTableEditable = false;
  TableConfig? tableConfig;
  int sortColumnIndex = 0;
  bool sortAscending = false;
  int rowsPerPage = 10;

  List<DataColumn> get dataColumns {
    return tableConfig!.columns
        .map((e) => DataColumn(
            label: Text(e.label),
            numeric: e.isNumeric,
            tooltip: e.tooltips,
            onSort: ((columnIndex, ascending) =>
                _sortTable(columnIndex, ascending))))
        .toList();
  }

  void _sortTable(int columnIndex, bool ascending) {
    //* TODO
  }

  @override
  Widget build(BuildContext context) {
    return _buildJetsDataTableWithScrollbars(context);
  }

  Widget _buildJetsDataTableWithScrollbars(BuildContext context) {
    return Scrollbar(
      thumbVisibility: true,
      trackVisibility: true,
      controller: _verticalController,
      child: SingleChildScrollView(
          scrollDirection: Axis.vertical,
          controller: _verticalController,
          child: Scrollbar(
            thumbVisibility: true,
            trackVisibility: true,
            controller: _horizontalController,
            child: SingleChildScrollView(
                scrollDirection: Axis.horizontal,
                controller: _horizontalController,
                padding: const EdgeInsets.all(defaultPadding),
                child: _buildJetsDataTable(context)),
          )),
    );
  }

  Widget _buildJetsDataTable(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          children: [
            ElevatedButton(
              style: ElevatedButton.styleFrom(
                // Foreground color
                onPrimary: Theme.of(context).colorScheme.onSecondaryContainer,
                // Background color
                primary: Theme.of(context).colorScheme.secondaryContainer,
              ).copyWith(elevation: ButtonStyleButton.allOrNull(0.0)),
              onPressed: () => _showDialog('Coming Soon!'),
              child: const Text('New Pipeline'),
            ),
            const SizedBox(width: defaultPadding),
            ElevatedButton(
              style: ElevatedButton.styleFrom(
                // Foreground color
                onPrimary: Theme.of(context).colorScheme.onSecondaryContainer,
                // Background color
                primary: Theme.of(context).colorScheme.secondaryContainer,
              ).copyWith(elevation: ButtonStyleButton.allOrNull(0.0)),
              onPressed: () {
                setState(() {
                  isTableEditable = !isTableEditable;
                });
              },
              child: const Text('Edit Table'),
            ),
          ],
        ),
        DataTable(
          columns: List<DataColumn>.generate(
              numItems,
              (int index) => DataColumn(
                    label: Text('Item $index'),
                  )),
          rows: List<DataRow>.generate(
            numItems,
            (int index) => DataRow.byIndex(
              index: index,
              color: MaterialStateProperty.resolveWith<Color?>(
                  (Set<MaterialState> states) {
                // All rows will have the same selected color.
                if (states.contains(MaterialState.selected)) {
                  return Theme.of(context)
                      .colorScheme
                      .primary
                      .withOpacity(0.08);
                }
                // Even rows will have a grey color.
                if (index.isEven) {
                  return Colors.grey.withOpacity(0.3);
                }
                return null; // Use default value for other states and odd rows.
              }),
              cells: List<DataCell>.generate(
                  numItems,
                  (int colIndex) =>
                      DataCell(Text('Cell row $index, col $colIndex'))),
              selected: selectedRows[index],
              onSelectChanged: isTableEditable
                  ? (bool? value) {
                      setState(() {
                        selectedRows[index] = value!;
                      });
                    }
                  : null,
            ),
          ),
        ),
      ],
    );
  }

  void _showDialog(String message) {
    showDialog<void>(
      context: context,
      builder: (context) => AlertDialog(
        title: Text(message),
        actions: [
          TextButton(
            child: const Text('OK'),
            onPressed: () => Navigator.of(context).pop(),
          ),
        ],
      ),
    );
  }
}