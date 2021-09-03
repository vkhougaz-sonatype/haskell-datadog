{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ViewPatterns               #-}

-- Source:
-- https://github.com/msgpack/msgpack-haskell/blob/master/msgpack-aeson/src/Data/MessagePack/Aeson.hs

-- | Aeson bridge for MessagePack
module MsgPack.Aeson (
  -- * Conversion functions
  toAeson, fromAeson,
  unsafeViaToJSON, viaFromJSON,

  -- * Wrapper instances
  AsMessagePack(..),
  AsAeson(..),
  MessagePackAesonError(..),

  -- * Utility functions
  packAeson, unpackAeson,
  decodeMessagePack, encodeMessagePack,
  ) where

import           Control.DeepSeq
import           Control.Exception
import           Data.Aeson               as A
import qualified Data.ByteString.Lazy     as L (ByteString)
import           Data.Data
import qualified Data.HashMap.Strict      as HM
import           Data.MessagePack         as MP
import           MsgPack.Integer
import           Data.Scientific
import qualified Data.Vector              as V

-- | Convert 'MP.Object' to JSON 'Value'
toAeson :: MP.Object -> A.Result Value
toAeson = \case
  ObjectNil      -> pure Null
  ObjectBool b   -> pure (Bool b)
  ObjectInt n    -> pure $! Number $! fromIntegral n
  ObjectFloat f  -> pure $! Number $! realToFrac f
  ObjectDouble d -> pure $! Number $! realToFrac d
  ObjectStr t    -> pure (String t)
  ObjectBin _    -> fail $ "ObjectBin is not supported by JSON"
  ObjectArray v  -> Array <$>V.mapM toAeson (V.fromList v)
  ObjectMap m    ->
    A.Object . HM.fromList . V.toList
      <$> V.mapM (\(k, v) -> (,) <$> from k <*> toAeson v) (V.fromList m)
      where from = aResult fail pure . MP.fromObject
  ObjectExt _ _  -> fail "ObjectExt is not supported by JSON"
  ObjectWord _  -> fail "ObjectWord is not supported by JSON"

-- | Convert JSON 'Value' to 'MP.Object'
fromAeson :: Value -> A.Result MP.Object
fromAeson = \case
  Null        -> pure ObjectNil
  Bool b      -> pure $ ObjectBool b
  Number s ->
    -- NOTE floatingOrInteger can OOM on untrusted input
    case floatingOrInteger s of
      Left n                            -> pure $ ObjectDouble n
      Right (fromIntegerTry -> Right n) -> pure $ ObjectInt (fromIntegral n)
      Right _                           -> fail "number out of bounds"
  String t    -> pure $ ObjectStr t
  Array v     -> ObjectArray <$> (V.toList <$> traverse fromAeson v)
  A.Object o  -> ObjectMap <$> traverse fromEntry (HM.toList o)
    where
      fromEntry (k, v) = (\a -> (ObjectStr k, a)) <$> fromAeson v


-- Helpers to piggyback off a JSON encoder / decoder when creating a MessagePack
-- instance.
--
-- Not as efficient as a direct encoder.
viaFromJSON :: FromJSON a => MP.Object -> A.Result a
viaFromJSON o = toAeson o >>= fromJSON

-- WARNING: not total for JSON numbers outside the 64 bit range
unsafeViaToJSON :: ToJSON a => a -> MP.Object
unsafeViaToJSON a = case fromAeson $ toJSON a of
  A.Error e   -> throw $ MessagePackAesonError e
  A.Success a -> a

data MessagePackAesonError = MessagePackAesonError String
  deriving (Eq, Show, Typeable)
instance Exception MessagePackAesonError

-- | Wrapper for using Aeson values as MessagePack value.
newtype AsMessagePack a = AsMessagePack { getAsMessagePack :: a }
  deriving (Eq, Ord, Show, Read, Functor, Data, Typeable, NFData)

instance (FromJSON a, ToJSON a) => MessagePack (AsMessagePack a) where
  fromObject o = AsMessagePack <$> (aResult fail pure (fromJSON =<< toAeson o))
  toObject = unsafeViaToJSON . getAsMessagePack

-- | Wrapper for using MessagePack values as Aeson value.
newtype AsAeson a = AsAeson { getAsAeson :: a }
  deriving (Eq, Ord, Show, Read, Functor, Data, Typeable, NFData)

instance MessagePack a => ToJSON (AsAeson a) where
  toJSON = aResult (const Null) id . toAeson . toObject . getAsAeson

instance MessagePack a => FromJSON (AsAeson a) where
  parseJSON j = case fromAeson j of
    A.Error e   -> fail e
    A.Success a -> aResult fail (pure . AsAeson) $ fromObject a

-- | Encode to MessagePack via "Data.Aeson"'s 'ToJSON' instances
packAeson :: ToJSON a => a -> A.Result L.ByteString
packAeson a = pack <$> (fromAeson $ toJSON a)

-- | Decode from MessagePack via "Data.Aeson"'s 'FromJSON' instances
unpackAeson :: FromJSON a => L.ByteString -> A.Result a
unpackAeson b = fromJSON =<< toAeson =<< either fail pure (unpack b)

-- | Encode MessagePack value to JSON document
encodeMessagePack :: MessagePack a => a -> L.ByteString
encodeMessagePack = encode . toJSON . AsAeson

-- | Decode MessagePack value from JSON document
decodeMessagePack :: MessagePack a => L.ByteString -> A.Result a
decodeMessagePack b = getAsAeson <$> (fromJSON =<< either A.Error A.Success (eitherDecode b))

aResult f s = \case
  A.Success a -> s a
  A.Error e   -> f e