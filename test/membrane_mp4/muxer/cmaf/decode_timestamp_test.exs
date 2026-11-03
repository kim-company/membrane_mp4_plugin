defmodule Membrane.MP4.Muxer.CMAF.DecodeTimestampTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.MP4.{Container, Helper}
  alias Membrane.Testing

  defmodule Origin do
    use Membrane.Filter
    def_options(owner: [spec: pid()], offset: [spec: integer()])
    def_input_pad(:input, flow_control: :auto, accepted_format: _any)
    def_output_pad(:output, flow_control: :auto, accepted_format: _any)
    @impl true
    def handle_init(_ctx, opts), do: {[], opts}
    @impl true
    def handle_buffer(:input, buffer, _ctx, opts) do
      buffer = %{buffer | pts: buffer.pts + opts.offset, dts: buffer.dts + opts.offset}
      send(opts.owner, {:sample, buffer})
      {[buffer: {:output, buffer}], opts}
    end
  end

  for origin <- [0, 200, 500, 3_600_000] do
    @origin origin
    test "B-frame decode time is serialized once at origin #{origin}ms" do
      owner = self()

      pipeline =
        Testing.Pipeline.start_link_supervised!(
          spec:
            child(:source, %Membrane.File.Source{
              location: Path.expand("../../../fixtures/in_video.h264", __DIR__)
            })
            |> child(:parser, %Membrane.H264.Parser{
              generate_best_effort_timestamps: %{framerate: {30, 1}, add_dts_offset: true},
              output_stream_structure: :avc1
            })
            |> child(:observe, %Origin{owner: owner, offset: Membrane.Time.milliseconds(@origin)})
            |> child(:muxer, %Membrane.MP4.Muxer.CMAF{
              segment_min_duration: Membrane.Time.seconds(2)
            })
            |> child(:sink, Testing.Sink)
        )

      assert_sink_buffer(pipeline, :sink, buffer)
      assert_receive {:sample, sample}
      refute sample.pts == sample.dts

      {boxes, <<>>} = Container.parse!(buffer.payload)
      traf = boxes[:moof].children[:traf].children
      decode_time = traf[:tfdt].fields.base_media_decode_time
      presentation_time = boxes[:sidx].fields.earliest_presentation_time
      timescale = boxes[:sidx].fields.timescale
      first_sample = hd(traf[:trun].fields.samples)

      offset = if sample.dts < 0, do: sample.pts - sample.dts, else: 0

      assert decode_time ==
               (sample.dts + offset) |> Helper.timescalify(timescale) |> Ratio.trunc()

      assert presentation_time == sample.pts |> Helper.timescalify(timescale) |> Ratio.trunc()

      assert first_sample.sample_composition_offset ==
               (sample.pts - sample.dts) |> Helper.timescalify(timescale) |> Ratio.trunc()

      assert_sink_buffer(pipeline, :sink, next)
      {next_boxes, <<>>} = Container.parse!(next.payload)

      next_decode_time =
        next_boxes[:moof].children[:traf].children[:tfdt].fields.base_media_decode_time

      duration = buffer.metadata.duration |> Helper.timescalify(timescale) |> Ratio.trunc()
      assert_in_delta next_decode_time - decode_time, duration, 1
      assert_end_of_stream(pipeline, :sink)
      Testing.Pipeline.terminate(pipeline)
    end
  end
end
