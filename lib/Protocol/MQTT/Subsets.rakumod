use v6.d;

unit package Protocol::MQTT:ver<0.0.1>:auth<cpan:LEONT>;

subset Byte is export of Int where 0 .. 255;
subset Short is export of Int where 0 .. 65535;

subset Topic is export of Str where /^ <-[\#\+]>* $ /;

