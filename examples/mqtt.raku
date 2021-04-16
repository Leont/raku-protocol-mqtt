#! raku

use Getopt::Long;
use Net::MQTT;
use Protocol::MQTT::Qos :qos;

sub io-supply(IO::Handle $handle) {
	my $supplier = Supplier.new;
	start {
		for $handle.lines -> $line {
			$supplier.emit($line);
		}
		$supplier.done;
	}
	return $supplier.Supply;
}

unit sub MAIN(Str $server = 'test.mosquitto.org', 
	Str :$client-identifier = 'raku-' ~ $*USER,
	Bool :$tls, 
	Qos :$qos = At-most-once,
	Str :$username,
	Str :$password,
	Int :$port = $tls ?? 8883 !! 1883,
	);

my $client = Net::MQTT.new(:$server, :$port, :$client-identifier, :$tls, :$username, :$password);
react {
	whenever $client.connected {
		say 'Connected';
		LAST { done }
	}
	whenever $client.disconnected -> $error {
		say $error;
	}
	whenever io-supply($*IN) {
		when / ^ sub[scribe]? \s+ $<topic>=[\S+] / {
			whenever $client.subscribe(~$<topic>, :$qos) -> (:$topic, :$payload, :$qos, :$retain) {
				say "$topic: {$payload.decode('utf8-c8')}";
			}
		}
		when / ^ unsub[scribe]? \s+ $<topic>=[\S+] / {
			whenever $client.unsubscribe(~$<topic>) {
				say "Unsubscribed $<topic>";
			}
		}
		when / ^ pub[lish]? \s+ $<topic>=[\S+] \s+ $<message>=[.*] $ / {
			whenever $client.publish(~$<topic>, ~$<message>, :$qos) {
				say 'Sent message';
			}
		}
		when / ^ ret[ain]? \s+ $<topic>=[\S+] \s+ $<message>=[.*] $ / {
			whenever $client.publish(~$<topic>, ~$<message>, :$qos, :retain) {
				say 'Sent retained message';
			}
		}
		when 'h'|'help' {
			say q:to/END/;
			sub <topic>
			unsub <topic>
			pub <topic> <message>
			ret <topic> <message>
			help
			quit
			END
		}
		when ''|'noop' {
		}
		when 'q'|'quit'|'disconnect' {
			$client.disconnect;
		}
		default {
			say "Couldn't parse '$_'";
		}
		LAST { done }
	}
}
