import json
import logging
import typing
import uuid
from typing import TYPE_CHECKING, Any, Iterable, List, Optional

import numpy as np
from llama_index.core.schema import BaseNode, MetadataMode, TextNode
from llama_index.core.vector_stores.types import (VectorStore,
                                                  VectorStoreQuery,
                                                  VectorStoreQueryResult)
from llama_index.core.vector_stores.utils import (legacy_metadata_dict_to_node,
                                                  metadata_dict_to_node,
                                                  node_to_metadata_dict)

import vearch
from vearch.config import Config
from vearch.core.vearch import Vearch
from vearch.schema.field import Field
from vearch.schema.index import HNSWIndex, ScalarIndex
from vearch.schema.space import SpaceSchema
from vearch.utils import DataType, MetricType, VectorInfo

logger = logging.getLogger(__name__)
_DEFAULT_TABLE_NAME = "liama_index_vearch"
_DEFAULT_CLUSTER_DB_NAME = "liama_index_vearch_client_db"


class VearchVectorStore(VectorStore):
    """
    Vearch vector store:
        embeddings are stored within a Vearch table.
        when query, the index uses Vearch to query for the top
        k most similar nodes.

    Args:
        chroma_collection (chromadb.api.models.Collection.Collection):
            ChromaDB collection instance
    """

    flat_metadata: bool = True
    stores_text: bool = True
    
    def __init__(
        self,
        path_or_url: Optional[str] = None,
        table_name: str = _DEFAULT_TABLE_NAME,
        db_name: str = _DEFAULT_CLUSTER_DB_NAME,
        **kwargs: Any,
    ) -> None:
        """
        Initialize vearch vector store
        """
        if path_or_url is None:
            raise ValueError("Please input url of cluster")

        if not db_name:
            db_name = self._DEFAULT_CLUSTER_DB_NAME
            db_name += "_"
            db_name += str(uuid.uuid4()).split("-")[-1]
        
        if not table_name:
            table_name = self._DEFAULT_TABLE_NAME
            table_name += "_"
            table_name += str(uuid.uuid4()).split("-")[-1]
            
        self.using_db_name = db_name
        self.using_table_name = table_name
        self.url = path_or_url
        self.vearch = Vearch(Config(host=path_or_url))
        

    @property
    def client(self) -> Any:
        """Get client."""
        return self._vearch

    def _get_matadata_field(self, metadatas: Optional[List[dict]] = None) -> None:
        field_list = []
        if metadatas:
            for key, value in metadatas[0].items():
                if isinstance(value, int):
                    field_list.append({"field": key, "type": "int"})
                    continue
                if isinstance(value, str):
                    field_list.append({"field": key, "type": "str"})
                    continue
                if isinstance(value, float):
                    field_list.append({"field": key, "type": "float"})
                    continue
                else:
                    raise ValueError("Please check data type,support int, str, float")
        self.field_list = field_list

    def _add_texts(
        self,
        ids: Iterable[str],
        texts: Iterable[str],
        metadatas: Optional[List[dict]] = None,
        embeddings: Optional[List[List[float]]] = None,
        **kwargs: Any,
    ) -> List[str]:
        """
        Returns:
            List of ids from adding the texts into the vectorstore.
        """
        if embeddings is None:
            raise ValueError("embeddings is None")
        self._get_matadata_field(metadatas)
        dbs= self._vearch.list_databases()
        dbs_list = [item.name["name"] for item in dbs]
        if self.using_db_name not in dbs_list:
            create_db_code = self._vearch.create_database(self.using_db_name)
            if create_db_code.code != 0:
                raise ValueError("create db failed!!!")
        spaces = self._vearch.list_spaces(self.using_db_name)
        space_list = [item.name["space_name"] for item in spaces]
        if self.using_table_name not in space_list:
            create_code = self._vearch.create_space(self.using_db_name, 
                self._create_space_schema(len(embeddings[0])))
            if create_code.code !=0 :
                raise ValueError("create space failed!!!")
        docid = []
        if embeddings is not None and metadatas is not None:
            meta_field_list = [i["field"] for i in self.field_list]
            for text, metadata, embed, id_d in zip(
                texts, metadatas, embeddings, ids
            ):
                profiles: typing.Dict[str, Any] = {}
                profiles["ref_doc_id"] = id_d
                profiles["text"] = text
                for f in meta_field_list:
                    profiles[f] = metadata[f]
                embed_np = np.array(embed)
                profiles["text_embedding"] = (embed_np / np.linalg.norm(embed_np)).tolist()
                insert_res = self._vearch.upsert(self.using_db_name, self.using_table_name, [profiles])
                if insert_res.code == 0:
                    docid.append(insert_res.document_ids[0]["_id"])
                    continue
                else:
                    retry_insert = self._vearch.upsert(
                        self.using_db_name, self.using_table_name, [profiles]
                    )
                    docid.append(retry_insert.document_ids[0]["_id"])
                    continue
        return docid

    def _create_space_schema(self, dim) ->SpaceSchema:
        filed_list_add = self.field_list + [{"field": "text", "type": "str"},{"field":"ref_doc_id","type":"str"}]
        type_dict = {"int": DataType.INTEGER, "str": DataType.STRING, 
                        "float": DataType.FLOAT}
        fields = [Field("text_embedding", DataType.VECTOR, 
            HNSWIndex("vec_idx", MetricType.Inner_product, 32, 64),dimension=dim)]
        for fi in filed_list_add:
            fields.append(Field(fi["field"], type_dict[fi["type"]], 
                index=ScalarIndex(fi["field"]+"_idx")))
        space_schema = SpaceSchema(self.using_table_name, fields)
        return space_schema


    def add(
        self,
        nodes: List[BaseNode],
        **add_kwargs: Any,
    ) -> List[str]:
        if not self._vearch:
            raise ValueError("Vearch Engine is not initialized")

        embeddings = []
        metadatas = []
        ids = []
        texts = []
        for node in nodes:
            embeddings.append(node.get_embedding())
            metadatas.append(
                node_to_metadata_dict(
                    node, remove_text=True, flat_metadata=self.flat_metadata
                )
            )
            ids.append(node.node_id)
            texts.append(node.get_content(metadata_mode=MetadataMode.NONE) or "")

        return self._add_texts(
            ids=ids,
            texts=texts,
            metadatas=metadatas,
            embeddings=embeddings,
        )

    def query(
        self,
        query: VectorStoreQuery,
        **kwargs: Any,
    ) -> VectorStoreQueryResult:
        """
        Query index for top k most similar nodes.

        Args:
            query : vector store query.

        Returns:
            VectorStoreQueryResult: Query results.
        """
        meta_filters = {}
        if query.filters is not None:
            for filter_ in query.filters.legacy_filters():
                meta_filters[filter_.key] = filter_.value
        _, _, schemas= self._vearch.is_space_exist(
            self.using_db_name, self.using_table_name
        )
        raw_fields = json.loads(schemas)["schema"]["fields"]
        meta_field_list = [item["name"] for item in raw_fields]
        meta_field_list.remove("text_embedding")
        embed = query.query_embedding
        if embed is None:
            raise ValueError("query.query_embedding is None")
        k = query.similarity_top_k
        vector = VectorInfo("text_embedding", (embed / np.linalg.norm(embed)).tolist())
        query_result = self._vearch.search(
            self.using_db_name, self.using_table_name, [vector,],
            fields = meta_field_list, limit = k)
        res = query_result.documents[0]
        nodes = []
        similarities = []
        ids = []
        for item in res:
            content = ""
            meta_data = {}
            node_id = ""
           
            score = item["_score"]
            for item_key in item:
                if item_key == "text":
                    content = item[item_key]
                    continue
                elif item_key == "_id":
                    node_id = item[item_key]
                    ids.append(node_id)
                    continue
                if item_key in meta_field_list:
                    meta_data[item_key] = item[item_key]
                    meta_field_list.remove(item_key)
                    continue
            similarities.append(score)
            try:
                node = metadata_dict_to_node(meta_data)
                node.set_content(content)
            except Exception:
                metadata, node_info, relationships = legacy_metadata_dict_to_node(
                    meta_data
                )
                node = TextNode(
                    text=content,
                    id_=node_id,
                    metadata=metadata,
                    start_char_idx=node_info.get("start", None),
                    end_char_idx=node_info.get("end", None),
                    relationships=relationships,
                )
            nodes.append(node)
        return VectorStoreQueryResult(nodes=nodes, similarities=similarities, ids=ids)

    def _delete(
        self,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> Optional[bool]:
        """
        Delete the documents which have the specified ids.

        Args:
            ids: The ids of the embedding vectors.
            **kwargs: Other keyword arguments that subclasses might use.

        Returns:
            Optional[bool]: True if deletion is successful.
            False otherwise, None if not implemented.
        """
        ret: Optional[bool] = None
        if ids is None or len(ids) == 0:
            return ret
        res = self.vearch.delete(self.using_db_name, self.using_table_name, ids)
        if res.code ==0:
            return True
        else:
            return False
        

    def delete(self, ref_doc_id: str, **delete_kwargs: Any) -> None:
        """Delete nodes using with ref_doc_id.

        Args:
            ref_doc_id (str): The doc_id of the document to delete.

        Returns:
            None
        """
        if len(ref_doc_id) == 0:
            return
        ids: List[str] = []
        ids.append(ref_doc_id)
        self._delete(ids)