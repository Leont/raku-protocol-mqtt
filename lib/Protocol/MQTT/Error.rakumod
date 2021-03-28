unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

our class Error is Exception is export(:exceptions) {
}

our class Error::Semantic is Error is export(:exceptions) {
	has Str $.message;
	method new($message) {
		return self.bless(:$message);
	}
}

our class Error::Decode is Error is export(:exceptions) {
}

our class Error::InsufficientData is Error::Decode is export(:exceptions) {
	has Str $.where;
	method new(Str $where) {
		return self.bless(:$where);
	}
	method message(--> Str) {
		return "$!where: insufficient data";
	}
}

our class Error::InvalidValue is Error::Decode is export(:exceptions) {
	has Str:D $.message is required;
	method new(Str $message = 'Invalid value') {
		return self.bless(:$message);
	}
}
