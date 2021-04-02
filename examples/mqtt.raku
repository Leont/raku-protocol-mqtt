#! raku

use v6.d;

use Protocol::MQTT::Client :state;
use Protocol::MQTT::Message;
use Protocol::MQTT::PacketBuffer;
use Protocol::MQTT::Subsets;
use Protocol::MQTT::Qos :qos;

use Getopt::Long;

class MQTT::Client {
	has IO::Socket::Async:U $!connection-class;
	has IO::Socket::Async $!connection;
	has Protocol::MQTT::Client $!client;
	has Protocol::MQTT::PacketBuffer $!decoder = Protocol::MQTT::PacketBuffer.new;
	has Protocol::MQTT::Dispatcher $!dispatcher = Protocol::MQTT::Dispatcher.new;
	has Cancellation $!cue;
	has Supplier  $!connected handles(:connected<Supply>)      = Supplier.new;
	has Supplier $!disconnected handles(:disconnected<Supply>) = Supplier.new;
	has Str $!server is required;
	has Int $!port is required;
	has Int:D $!connect-interval is required;
	has Int:D $!reconnect-attempts is required;
	has Int:D $!reconnect-tried is required;

	submethod BUILD(
		Str:D :$!server!,
		Str:D :$client-identifier!,
		Bool  :$tls,
		Int:D :$!port               = $tls ?? 8883 !! 1883,
		Int:D :$keep-alive-interval = 6,
		Int:D :$resend-interval     = 1,
		Int:D :$!connect-interval   = $resend-interval,
		Int:D :$!reconnect-attempts = 3;
		Str   :$username,
		Str   :password($password-string),
		Protocol::MQTT::Message :$will;
		) {
		die 'Oversized keep-alive interval' if $keep-alive-interval !~~ Short;

		my Blob $password = $password-string.defined ?? $password-string.encode !! Nil;
		$!client = Protocol::MQTT::Client.new(:$client-identifier, :$keep-alive-interval, :$resend-interval, :$!connect-interval, :$username, :$password, :$will);
		$!client.incoming.tap: -> $message {
			$!dispatcher.dispatch($message);
		}
		$!client.disconnected.tap: -> $cause {
			self!disconnected;
		}

		$!connection-class = $tls ?? (require IO::Socket::Async::SSL) !! IO::Socket::Async;
		$!reconnect-tried = 0;
		self!connect;
	}

	method !connect(--> Nil) {
		$!connection-class.connect($!server, $!port).then: -> $connecting {
			if $connecting.status ~~ Kept {
				$!connection = $connecting.result;
				$!reconnect-tried = 0;

				my $connect-promise = $!client.connect;
				$connect-promise.then: -> $success {
					if $success.status ~~ Kept  {
						$!connected.emit($success.result);
					}
					else {
						self!disconnected;
					}
				}
				self!send-events(now);

				sub parser(buf8 $received) {
					my $now = now;
					$!decoder.add-data($received);
					while $!decoder.get-packet -> $packet {
						$!client.received-packet($packet, $now);
					}
					self!send-events($now);
				}
				sub done {
					$!cue.cancel with $!cue;
					$!disconnected.emit('Disconnected');
					if self.$!reconnect-tried < $!reconnect-attempts {
						self!connect;
						$!client.connect;
						self!send-events(now);
					}
				}
				$!connection.Supply(:bin).tap(&parser, :&done);
			}
			elsif self.$!reconnect-tried++ < $!reconnect-attempts {
				my $now = now;
				my $at = $now + $!connect-interval;
				$!cue = $*SCHEDULER.cue({ self!connect unless $!connection }, :$at)
			}
			else {
				$!connected.done;
				$!disconnected.emit('Couldn\'t connect');
			}
		}
	}

	method !disconnected(--> Nil) {
		my $connection = $!connection;
		$!connection = Nil;
		$connection.close;
	}

	method !send-events(Instant $now --> Nil) {
		$!cue.cancel with $!cue;
		for $!client.next-events($now) -> $message {
			$!connection.write: $message.encode;
		}
		my $at = $!client.next-expiration;

		my $callback = $!client.state === Unconnected ?? { self!connect } !! { self!send-events(now) }
		$!cue = $at ?? $*SCHEDULER.cue($callback, :$at) !! Nil;
	}

	method publish(Str:D $topic, Str:D $message, Bool:D :$retain = False, Qos:D :$qos = At-most-once --> Promise:D) {
		my $now = now;
		return Promise.broken('Invalid topic name') if $topic !~~ Topic;
		my $result = $!client.publish($topic, $message.encode, $qos, $retain, now);
		self!send-events($now);
		return $result;
	}

	method subscribe(Str:D $topic, Qos:D :$qos = At-most-once --> Supply:D) {
		my $now = now;
		my $result = $!dispatcher.add-filter($topic);
		$!client.subscribe($topic, $qos, $now);
		self!send-events($now);
		return $result;
	}

	method unsubscribe(Str:D $topic --> Promise:D) {
		my $now = now;
		$!dispatcher.remove-filter($topic);
		my $result = $!client.unsubscribe($topic, $now);
		self!send-events($now);
		return $result;
	}

	method disconnect(--> Nil) {
		$!reconnect-attempts = 0;
		$!client.disconnect;
		self!send-events(now);
		$!connection.close;
		$!connected.done;
	}
}

sub io-supply(IO::Handle $handle) {
	my $supplier = Supplier.new;
	start {
		react {
			whenever $*IN.Supply(:1size) -> $input {
				$supplier.emit($input);
			}
		}
	}
	return $supplier.Supply;
}

sub MAIN(Str $server = 'test.mosquitto.org', 
	Str :$client-identifier = 'raku-' ~ $*USER,
	Bool :$tls, 
	Qos :$qos = At-most-once,
	Str :$username,
	Str :$password,
	Int :$port = $tls ?? 8883 !! 1883,
	) {
	my $client = MQTT::Client.new(:$server, :$port, :$client-identifier, :$tls, :$username, :$password);
	react {
		whenever $client.connected {
			say 'Connected';
			LAST { done }
		}
		whenever $client.disconnected -> $error {
			say $error;
		}
		whenever io-supply($*IN).lines {
			when / ^ sub[scribe]? \s+ $<topic>=[\S+] / {
				whenever $client.subscribe(~$<topic>, :$qos) -> (:$topic, :$message, :$qos, :$retain) {
					say "$topic: {$message.decode('utf8-c8')}";
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
		}
	}
}
