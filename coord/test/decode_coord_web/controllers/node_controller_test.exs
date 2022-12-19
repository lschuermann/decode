defmodule DecodeCoordWeb.NodeControllerTest do
  use DecodeCoordWeb.ConnCase

  setup %{conn: conn} do
    {:ok, conn: put_req_header(conn, "accept", "application/json")}
  end

  describe "register node" do
    test "registers a new node with a given UUID, URL and a shard", %{conn: conn} do
      node_id = UUID.uuid4()

      conn =
        put(conn, Routes.node_path(conn, :register, node_id), %{
          node_url: "http://localhost:8000/",
          shards: []
        })

      assert response(conn, 200)
    end
  end

  # describe "index" do
  #   test "lists all node", %{conn: conn} do
  #     conn = get(conn, Routes.node_path(conn, :index))
  #     assert json_response(conn, 200)["data"] == []
  #   end
  # end

  # describe "create node" do
  #   test "renders node when data is valid", %{conn: conn} do
  #     conn = post(conn, Routes.node_path(conn, :create), node: @create_attrs)
  #     assert %{"id" => id} = json_response(conn, 201)["data"]

  #     conn = get(conn, Routes.node_path(conn, :show, id))

  #     assert %{
  #              "id" => ^id
  #            } = json_response(conn, 200)["data"]
  #   end

  #   test "renders errors when data is invalid", %{conn: conn} do
  #     conn = post(conn, Routes.node_path(conn, :create), node: @invalid_attrs)
  #     assert json_response(conn, 422)["errors"] != %{}
  #   end
  # end

  # describe "update node" do
  #   setup [:create_node]

  #   test "renders node when data is valid", %{conn: conn, node: %Node{id: id} = node} do
  #     conn = put(conn, Routes.node_path(conn, :update, node), node: @update_attrs)
  #     assert %{"id" => ^id} = json_response(conn, 200)["data"]

  #     conn = get(conn, Routes.node_path(conn, :show, id))

  #     assert %{
  #              "id" => ^id
  #            } = json_response(conn, 200)["data"]
  #   end

  #   test "renders errors when data is invalid", %{conn: conn, node: node} do
  #     conn = put(conn, Routes.node_path(conn, :update, node), node: @invalid_attrs)
  #     assert json_response(conn, 422)["errors"] != %{}
  #   end
  # end

  # describe "delete node" do
  #   setup [:create_node]

  #   test "deletes chosen node", %{conn: conn, node: node} do
  #     conn = delete(conn, Routes.node_path(conn, :delete, node))
  #     assert response(conn, 204)

  #     assert_error_sent 404, fn ->
  #       get(conn, Routes.node_path(conn, :show, node))
  #     end
  #   end
  # end

  # defp create_node(_) do
  #   node = node_fixture()
  #   %{node: node}
  # end
end
