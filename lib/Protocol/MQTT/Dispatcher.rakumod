use v6.d;

unit class Protocol::MQTT::Dispatcher:ver<0.0.1>:auth<cpan:LEONT>;

use Protocol::MQTT::Filter;
use Protocol::MQTT::Message;

my class Matcher {
	has Protocol::MQTT::Filter:D $.filter is required;
	has Supplier:D $.supplier is required;
}

has Matcher %!matchers;

method add-filter(Str $topic --> Supply:D) {
	if %!matchers{$topic} -> $existing {
		return $existing.supplier.Supply;
	}
	else {
		my $supplier = Supplier.new;
		my $filter = Protocol::MQTT::Filter.new(:$topic);
		%!matchers{$topic} = Matcher.new(:$filter, :$supplier);
		return $supplier.Supply;
	}
}

method remove-filter(Str $topic --> Nil) {
	if %!matchers{$topic} -> $existing {
		%!matchers{$topic}.supplier.done;
		%!matchers{$topic}:delete;
	}
}

method dispatch(Protocol::MQTT::Message:D $message --> Nil) {
	for %!matchers.values -> (:$supplier, :$filter) {
		$supplier.emit($message) if $message.topic ~~ $filter;
	}
}

method clear(--> Nil) {
	for %!matchers.values -> $matcher {
		$matcher.supplier.done;
	}
	%!matchers = ();
}
