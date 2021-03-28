use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

our class Filter {
	my sub to-matcher(Str $filter) {
		return !*.starts-with('$') if $filter eq '#';
		return *.starts-with('/') if $filter eq '/#';

		my $anchor = True;
		my @matchers;
		@matchers.push: /<!before '$'>/ if $filter ~~ / ^ \+ /;
		for $filter.comb(/ '/#' | '/' | <-[/]>+ /) {
			when '/#' { $anchor = False; last }
			when '+'  { @matchers.push: /<-[/]>*/ }
			default   { @matchers.push: $_ }
		}
		if all(@matchers) ~~ Str {
			my $string = @matchers.join;
			return $anchor ?? $string !! *.starts-with($string);
		}
		elsif $anchor {
			@matchers.push: / $ /;
		}
		@matchers.unshift: /^/;
		return @matchers.reduce({ /$^a$^b/ });
	}

	has Str:D $.topic is required;
	has Any:D $!matcher handles<ACCEPTS> = to-matcher($!topic);
}
