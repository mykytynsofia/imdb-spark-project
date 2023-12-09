part of 'main_cubit.dart';

class MainState extends Equatable {
  final Status status;
  final List<dynamic> data;

  const MainState({
    required this.status,
    required this.data,
  });

  MainState copyWith({
    Status? status,
    List<dynamic>? data,
  }) {
    return MainState(
      status: status ?? this.status,
      data: data ?? this.data,
    );
  }

  @override
  List<Object> get props => [status, data];
}

enum Status { initial, loading, success }
